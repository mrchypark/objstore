// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package swift

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore/errutil"
)

func TestParseConfig(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
username: thanos
user_domain_name: userDomain
project_name: thanosProject
project_domain_name: projectDomain`)

	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	testutil.Equals(t, "http://identity.something.com/v3", cfg.AuthUrl)
	testutil.Equals(t, "thanos", cfg.Username)
	testutil.Equals(t, "userDomain", cfg.UserDomainName)
	testutil.Equals(t, "thanosProject", cfg.ProjectName)
	testutil.Equals(t, "projectDomain", cfg.ProjectDomainName)
}

func TestParseConfigFail(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
tenant_name: something`)

	_, err := parseConfig(input)
	// Must result in unmarshal error as there's no `tenant_name` in SwiftConfig.
	testutil.NotOk(t, err)
}

func TestParseConfig_HTTPConfig(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
username: thanos
user_domain_name: userDomain
project_name: thanosProject
project_domain_name: projectDomain
http_config:
  tls_config:
    ca_file: /certs/ca.crt
    cert_file: /certs/cert.crt
    key_file: /certs/key.key
    server_name: server
    insecure_skip_verify: false`)
	cfg, err := parseConfig([]byte(input))

	testutil.Ok(t, err)

	testutil.Equals(t, "http://identity.something.com/v3", cfg.AuthUrl)
	testutil.Equals(t, "thanos", cfg.Username)
	testutil.Equals(t, "userDomain", cfg.UserDomainName)
	testutil.Equals(t, "thanosProject", cfg.ProjectName)
	testutil.Equals(t, "projectDomain", cfg.ProjectDomainName)
	testutil.Equals(t, model.Duration(90*time.Second), cfg.HTTPConfig.IdleConnTimeout)
	testutil.Equals(t, model.Duration(2*time.Minute), cfg.HTTPConfig.ResponseHeaderTimeout)
	testutil.Equals(t, false, cfg.HTTPConfig.InsecureSkipVerify)

}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	config := DefaultConfig
	config.AuthUrl = "http://identity.something.com/v3"
	_, err := NewContainerFromConfig(log.NewNopLogger(), &config, false, errutil.WrapWithErrRoundtripper)

	// We expect an error from the RoundTripper
	testutil.NotOk(t, err)
	testutil.Assert(t, errutil.IsMockedError(err), "Expected RoundTripper error, got: %v", err)
}

func TestSwiftMetadataSupport(t *testing.T) {
	// 이 테스트는 실제 Swift 환경이 필요하므로 스킵합니다
	// 실제 환경에서 테스트하려면 적절한 Swift 설정이 필요합니다
	t.Skip("Swift metadata test requires actual Swift environment")

	// 실제 테스트 코드는 다음과 같습니다:
	/*
		container, closeFn, err := NewTestContainer(t)
		testutil.Ok(t, err)
		defer closeFn()

		ctx := context.Background()
		objectName := "test-metadata-object"
		content := "test content for metadata"

		// 메타데이터와 함께 객체 업로드
		metadata := map[string]string{
			"author":      "test-user",
			"version":     "1.0",
			"environment": "testing",
		}

		err = container.Upload(ctx, objectName, strings.NewReader(content),
			objstore.WithUserMetadata(metadata))
		testutil.Ok(t, err)

		// 객체 속성 조회 및 메타데이터 확인
		attrs, err := container.Attributes(ctx, objectName)
		testutil.Ok(t, err)

		// 메타데이터 검증
		testutil.Assert(t, attrs.UserMetadata != nil, "UserMetadata should not be nil")
		testutil.Equals(t, "test-user", attrs.UserMetadata["author"])
		testutil.Equals(t, "1.0", attrs.UserMetadata["version"])
		testutil.Equals(t, "testing", attrs.UserMetadata["environment"])

		// ETag 검증
		testutil.Assert(t, attrs.ETag != "", "ETag should not be empty")

		// 메타데이터 없이 업로드한 객체 테스트
		objectName2 := "test-no-metadata-object"
		err = container.Upload(ctx, objectName2, strings.NewReader(content))
		testutil.Ok(t, err)

		attrs2, err := container.Attributes(ctx, objectName2)
		testutil.Ok(t, err)

		// 메타데이터가 없어야 함
		testutil.Assert(t, attrs2.UserMetadata == nil, "UserMetadata should be nil for object without metadata")
		testutil.Assert(t, attrs2.ETag != "", "ETag should not be empty even without metadata")
	*/
}
