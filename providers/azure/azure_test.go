// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package azure

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/errutil"
	"github.com/thanos-io/objstore/exthttp"
)

type TestCase struct {
	name             string
	config           []byte
	wantFailParse    bool
	wantFailValidate bool
}

var validConfig = []byte(`storage_account: "myStorageAccount"
storage_account_key: "bXlTdXBlclNlY3JldEtleTEyMyFAIw=="
container: "MyContainer"
endpoint: "blob.core.windows.net"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": 0`)

var tests = []TestCase{
	{
		name:             "validConfig",
		config:           validConfig,
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "Missing storage account",
		config: []byte(`storage_account: ""
storage_account_key: "abc123"
container: "MyContainer"
endpoint: "blob.core.windows.net"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": 0`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Negative max_tries",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "max_tries": -1
  "try_timeout": 0`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Negative max_retry_requests",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": -100
pipeline_config:
  "try_timeout": 0`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Not a Duration",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": 10`),
		wantFailParse:    true,
		wantFailValidate: true,
	},
	{
		name: "Valid Duration",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": "10s"`),
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "Valid MSI Resource",
		config: []byte(`storage_account: "myAccount"
storage_account_key: ""
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": "10s"`),
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "Valid User Assigned Identity Config without Resource",
		config: []byte(`storage_account: "myAccount"
storage_account_key: ""
user_assigned_id: "1234-56578678-655"
container: "MyContainer"`),
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "Valid User Assigned Identity Config with Resource",
		config: []byte(`storage_account: "myAccount"
storage_account_key: ""
user_assigned_id: "1234-56578678-655"
container: "MyContainer"`),
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "Valid User Assigned and Connection String set",
		config: []byte(`storage_account: "myAccount"
storage_account_key: ""
user_assigned_id: "1234-56578678-655"
storage_connection_string: "myConnectionString"
container: "MyContainer"`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Valid AzTenantID, ClientID, ClientSecret",
		config: []byte(`storage_account: "myAccount"
storage_account_key: ""
az_tenant_id: "1234-56578678-655"
client_id: "1234-56578678-655"
client_secret: "1234-56578678-655"
container: "MyContainer"`),
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "Valid ClientID and ClientSecret but missing AzTenantID",
		config: []byte(`storage_account: "myAccount"
storage_account_key: ""
client_id: "1234-56578678-655"
client_secret: "1234-56578678-655"
container: "MyContainer"`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
}

func TestConfig_validate(t *testing.T) {

	for _, testCase := range tests {

		conf, err := parseConfig(testCase.config)

		if (err != nil) != testCase.wantFailParse {
			t.Errorf("%s error = %v, wantFailParse %v", testCase.name, err, testCase.wantFailParse)
			continue
		}

		validateErr := conf.validate()
		if (validateErr != nil) != testCase.wantFailValidate {
			t.Errorf("%s error = %v, wantFailValidate %v", testCase.name, validateErr, testCase.wantFailValidate)
		}
	}

}

func TestParseConfig_DefaultHTTPConfig(t *testing.T) {

	cfg, err := parseConfig(validConfig)
	testutil.Ok(t, err)

	if time.Duration(cfg.HTTPConfig.IdleConnTimeout) != time.Duration(90*time.Second) {
		t.Errorf("parsing of idle_conn_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(90*time.Second))
	}

	if time.Duration(cfg.HTTPConfig.ResponseHeaderTimeout) != time.Duration(2*time.Minute) {
		t.Errorf("parsing of response_header_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(2*time.Minute))
	}

	if cfg.HTTPConfig.InsecureSkipVerify {
		t.Errorf("parsing of insecure_skip_verify failed: got %v, expected %v", cfg.HTTPConfig.InsecureSkipVerify, false)
	}
}

func TestParseConfig_CustomHTTPConfigWithTLS(t *testing.T) {
	input := []byte(`storage_account: "myStorageAccount"
storage_account_key: "abc123"
container: "MyContainer"
endpoint: "blob.core.windows.net"
http_config:
  tls_config:
    ca_file: /certs/ca.crt
    cert_file: /certs/cert.crt
    key_file: /certs/key.key
    server_name: server
    insecure_skip_verify: false
  `)
	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	testutil.Equals(t, "/certs/ca.crt", cfg.HTTPConfig.TLSConfig.CAFile)
	testutil.Equals(t, "/certs/cert.crt", cfg.HTTPConfig.TLSConfig.CertFile)
	testutil.Equals(t, "/certs/key.key", cfg.HTTPConfig.TLSConfig.KeyFile)
	testutil.Equals(t, "server", cfg.HTTPConfig.TLSConfig.ServerName)
	testutil.Equals(t, false, cfg.HTTPConfig.TLSConfig.InsecureSkipVerify)
}

func TestParseConfig_CustomLegacyInsecureSkipVerify(t *testing.T) {
	input := []byte(`storage_account: "myStorageAccount"
storage_account_key: "abc123"
container: "MyContainer"
endpoint: "blob.core.windows.net"
http_config:
  insecure_skip_verify: true
  tls_config:
    insecure_skip_verify: false
  `)
	cfg, err := parseConfig(input)
	testutil.Ok(t, err)
	transport, err := exthttp.DefaultTransport(cfg.HTTPConfig)
	testutil.Ok(t, err)
	testutil.Equals(t, true, transport.TLSClientConfig.InsecureSkipVerify)
}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	cfg, err := parseConfig(validConfig)
	testutil.Ok(t, err)

	_, err = NewBucketWithConfig(log.NewNopLogger(), cfg, "test", errutil.WrapWithErrRoundtripper)

	// We expect an error from the RoundTripper
	testutil.NotOk(t, err)
	testutil.Assert(t, errutil.IsMockedError(err), "Expected RoundTripper error, got: %v", err)
}

func TestAzureMetadataSupport(t *testing.T) {
	// 이 테스트는 실제 Azure 환경이 필요하므로 환경 변수가 설정된 경우에만 실행
	if os.Getenv("AZURE_STORAGE_ACCOUNT") == "" || os.Getenv("AZURE_STORAGE_ACCESS_KEY") == "" {
		t.Skip("Azure credentials not provided, skipping metadata integration tests")
	}

	ctx := context.Background()

	// 테스트용 버킷 생성
	bkt, closeFn, err := NewTestBucket(t, "test-metadata")
	testutil.Ok(t, err)
	defer closeFn()

	// 테스트 데이터 준비
	testObjectName := "test-metadata-object"
	testContent := "test content for metadata"
	testMetadata := map[string]string{
		"author":      "test-user",
		"version":     "1.0",
		"environment": "testing",
		"project":     "thanos-objstore",
	}

	t.Run("upload with metadata", func(t *testing.T) {
		// 메타데이터와 함께 객체 업로드
		err := bkt.Upload(ctx, testObjectName, strings.NewReader(testContent),
			objstore.WithUserMetadata(testMetadata))
		testutil.Ok(t, err)
	})

	t.Run("retrieve metadata and etag", func(t *testing.T) {
		// 객체 속성 조회
		attrs, err := bkt.Attributes(ctx, testObjectName)
		testutil.Ok(t, err)

		// 기본 속성 검증
		testutil.Equals(t, int64(len(testContent)), attrs.Size)
		testutil.Assert(t, !attrs.LastModified.IsZero(), "LastModified should be set")

		// ETag 검증
		testutil.Assert(t, attrs.ETag != "", "ETag should not be empty")
		testutil.Assert(t, len(attrs.ETag) > 0, "ETag should have content")

		// 메타데이터 검증
		testutil.Assert(t, attrs.UserMetadata != nil, "UserMetadata should not be nil")
		testutil.Equals(t, len(testMetadata), len(attrs.UserMetadata))

		// 각 메타데이터 키-값 쌍 검증 (Azure는 키를 소문자로 변환)
		for key, expectedValue := range testMetadata {
			normalizedKey := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
			actualValue, exists := attrs.UserMetadata[normalizedKey]
			testutil.Assert(t, exists, "Metadata key '%s' (normalized: '%s') should exist", key, normalizedKey)
			testutil.Equals(t, expectedValue, actualValue)
		}
	})

	t.Run("upload without metadata", func(t *testing.T) {
		testObjectNameNoMeta := "test-no-metadata-object"

		// 메타데이터 없이 객체 업로드
		err := bkt.Upload(ctx, testObjectNameNoMeta, strings.NewReader(testContent))
		testutil.Ok(t, err)

		// 객체 속성 조회
		attrs, err := bkt.Attributes(ctx, testObjectNameNoMeta)
		testutil.Ok(t, err)

		// 메타데이터가 없어야 함
		testutil.Assert(t, attrs.UserMetadata == nil || len(attrs.UserMetadata) == 0,
			"UserMetadata should be empty when not provided")

		// ETag는 여전히 존재해야 함
		testutil.Assert(t, attrs.ETag != "", "ETag should exist even without metadata")

		// 정리
		err = bkt.Delete(ctx, testObjectNameNoMeta)
		testutil.Ok(t, err)
	})

	t.Run("etag consistency", func(t *testing.T) {
		testObjectNameETag := "test-etag-consistency"

		// 동일한 콘텐츠로 두 번 업로드
		err := bkt.Upload(ctx, testObjectNameETag, strings.NewReader(testContent))
		testutil.Ok(t, err)

		attrs1, err := bkt.Attributes(ctx, testObjectNameETag)
		testutil.Ok(t, err)

		// 같은 객체를 다시 업로드
		err = bkt.Upload(ctx, testObjectNameETag, strings.NewReader(testContent))
		testutil.Ok(t, err)

		attrs2, err := bkt.Attributes(ctx, testObjectNameETag)
		testutil.Ok(t, err)

		// ETag가 변경되었는지 확인 (Azure는 업로드할 때마다 새로운 ETag 생성)
		testutil.Assert(t, attrs1.ETag != "", "First ETag should not be empty")
		testutil.Assert(t, attrs2.ETag != "", "Second ETag should not be empty")

		// 정리
		err = bkt.Delete(ctx, testObjectNameETag)
		testutil.Ok(t, err)
	})

	t.Run("invalid metadata handling", func(t *testing.T) {
		testObjectNameInvalid := "test-invalid-metadata"

		// 잘못된 메타데이터로 업로드 시도
		invalidMetadata := map[string]string{
			"": "empty-key", // 빈 키
		}

		err := bkt.Upload(ctx, testObjectNameInvalid, strings.NewReader(testContent),
			objstore.WithUserMetadata(invalidMetadata))
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), "invalid user metadata"),
			"Error should mention invalid metadata")
	})

	t.Run("large metadata handling", func(t *testing.T) {
		testObjectNameLarge := "test-large-metadata"

		// 큰 메타데이터 생성
		largeMetadata := make(map[string]string)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key_%d", i)
			value := strings.Repeat("x", 100) // 100자 값
			largeMetadata[key] = value
		}

		err := bkt.Upload(ctx, testObjectNameLarge, strings.NewReader(testContent),
			objstore.WithUserMetadata(largeMetadata))

		// 메타데이터가 너무 클 경우 에러가 발생해야 함
		if err != nil {
			testutil.Assert(t, strings.Contains(err.Error(), "metadata too large") ||
				strings.Contains(err.Error(), "invalid user metadata"),
				"Error should be related to metadata size")
		} else {
			// 업로드가 성공한 경우 정리
			err = bkt.Delete(ctx, testObjectNameLarge)
			testutil.Ok(t, err)
		}
	})

	// 테스트 정리
	err = bkt.Delete(ctx, testObjectName)
	testutil.Ok(t, err)
}
