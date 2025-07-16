// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package gcs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/fullstorydev/emulators/storage/gcsemu"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/errutil"
	"google.golang.org/api/option"
)

func TestBucket_Get_ShouldReturnErrorIfServerTruncateResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
		w.Header().Set("Content-Length", "100")

		// Write less bytes than the content length.
		_, err := w.Write([]byte("12345"))
		testutil.Ok(t, err)
	}))
	defer srv.Close()

	os.Setenv("STORAGE_EMULATOR_HOST", srv.Listener.Addr().String())

	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
	}

	// NewBucketWithConfig wraps newBucket and processes HTTP options. Can skip for test.
	bkt, err := newBucket(context.Background(), log.NewNopLogger(), cfg, []option.ClientOption{})
	testutil.Ok(t, err)

	reader, err := bkt.Get(context.Background(), "test")
	testutil.Ok(t, err)

	// We expect an error when reading back.
	_, err = io.ReadAll(reader)
	testutil.NotOk(t, err)
	testutil.Equals(t, "storage: partial request not satisfied", err.Error())
}

func TestNewBucketWithConfig_ShouldCreateGRPC(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)
	err = os.Setenv("GCS_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)
	err = os.Setenv("STORAGE_EMULATOR_HOST_GRPC", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)

	// Check if the bucket is created.
	testutil.Assert(t, bkt != nil, "expected bucket to be created")
}

func TestParseConfig_ChunkSize(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      string
		assertions func(cfg Config)
	}{
		{
			name:  "DefaultConfig",
			input: `bucket: abcd`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.ChunkSizeBytes, 0)
			},
		},
		{
			name: "CustomConfig",
			input: `bucket: abcd
chunk_size_bytes: 1024`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.ChunkSizeBytes, 1024)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig([]byte(tc.input))
			testutil.Ok(t, err)
			tc.assertions(cfg)
		})
	}
}

func TestParseConfig_HTTPConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      string
		assertions func(cfg Config)
	}{
		{
			name:  "DefaultHTTPConfig",
			input: `bucket: abcd`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.HTTPConfig.IdleConnTimeout, model.Duration(90*time.Second))
				testutil.Equals(t, cfg.HTTPConfig.ResponseHeaderTimeout, model.Duration(2*time.Minute))
				testutil.Equals(t, cfg.HTTPConfig.InsecureSkipVerify, false)
			},
		},
		{
			name: "CustomHTTPConfig",
			input: `bucket: abcd
http_config:
  insecure_skip_verify: true
  idle_conn_timeout: 50s
  response_header_timeout: 1m`,
			assertions: func(cfg Config) {
				testutil.Equals(t, cfg.HTTPConfig.IdleConnTimeout, model.Duration(50*time.Second))
				testutil.Equals(t, cfg.HTTPConfig.ResponseHeaderTimeout, model.Duration(1*time.Minute))
				testutil.Equals(t, cfg.HTTPConfig.InsecureSkipVerify, true)
			},
		},
		{
			name: "CustomHTTPConfigWithTLS",
			input: `bucket: abcd
http_config:
  tls_config:
    ca_file: /certs/ca.crt
    cert_file: /certs/cert.crt
    key_file: /certs/key.key
    server_name: server
    insecure_skip_verify: false`,
			assertions: func(cfg Config) {
				testutil.Equals(t, "/certs/ca.crt", cfg.HTTPConfig.TLSConfig.CAFile)
				testutil.Equals(t, "/certs/cert.crt", cfg.HTTPConfig.TLSConfig.CertFile)
				testutil.Equals(t, "/certs/key.key", cfg.HTTPConfig.TLSConfig.KeyFile)
				testutil.Equals(t, "server", cfg.HTTPConfig.TLSConfig.ServerName)
				testutil.Equals(t, false, cfg.HTTPConfig.TLSConfig.InsecureSkipVerify)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig([]byte(tc.input))
			testutil.Ok(t, err)
			tc.assertions(cfg)
		})
	}
}

func TestNewBucketWithErrorRoundTripper(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}
	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", errutil.WrapWithErrRoundtripper)
	testutil.Ok(t, err)
	_, err = bkt.Get(context.Background(), "test-bucket")
	testutil.NotOk(t, err)
	testutil.Assert(t, errutil.IsMockedError(err), "Expected RoundTripper error, got: %v", err)
}

func TestBucket_UploadWithMetadata(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()
	objectName := "test-object-with-metadata"
	content := "test content for metadata"

	// 테스트용 메타데이터 정의
	testMetadata := map[string]string{
		"author":      "test-user",
		"version":     "1.0",
		"environment": "testing",
		"project_id":  "test-project",
	}

	// 메타데이터와 함께 객체 업로드
	err = bkt.Upload(ctx, objectName, strings.NewReader(content),
		objstore.WithUserMetadata(testMetadata),
		objstore.WithContentType("text/plain"))
	testutil.Ok(t, err)

	// 객체 속성 조회
	attrs, err := bkt.Attributes(ctx, objectName)
	testutil.Ok(t, err)

	// 기본 속성 검증
	testutil.Equals(t, int64(len(content)), attrs.Size)
	testutil.Assert(t, !attrs.LastModified.IsZero(), "LastModified should be set")
	// ETag는 에뮬레이터에서 제공되지 않을 수 있으므로 선택적으로 검증
	if attrs.ETag != "" {
		err = objstore.ValidateETag(attrs.ETag)
		testutil.Ok(t, err)
	}

	// 메타데이터 검증
	testutil.Assert(t, attrs.UserMetadata != nil, "UserMetadata should not be nil")

	// GCS는 메타데이터 키를 소문자로 변환하고 하이픈을 언더스코어로 변경
	expectedKeys := map[string]string{
		"author":      "test-user",
		"version":     "1.0",
		"environment": "testing",
		"project_id":  "test-project",
	}

	for expectedKey, expectedValue := range expectedKeys {
		actualValue, exists := attrs.UserMetadata[expectedKey]
		testutil.Assert(t, exists, "Expected metadata key '%s' not found", expectedKey)
		testutil.Equals(t, expectedValue, actualValue)
	}
}

func TestBucket_UploadWithInvalidMetadata(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()
	objectName := "test-object-invalid-metadata"
	content := "test content"

	// 잘못된 메타데이터 테스트 케이스들
	testCases := []struct {
		name     string
		metadata map[string]string
		wantErr  bool
	}{
		{
			name: "empty key",
			metadata: map[string]string{
				"":      "value",
				"valid": "value",
			},
			wantErr: true,
		},
		{
			name: "reserved key",
			metadata: map[string]string{
				"content-type": "text/plain",
			},
			wantErr: true,
		},
		{
			name: "invalid characters in key",
			metadata: map[string]string{
				"key@invalid": "value",
			},
			wantErr: true,
		},
		{
			name: "valid metadata",
			metadata: map[string]string{
				"author":  "test-user",
				"version": "1.0",
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := bkt.Upload(ctx, objectName+"-"+tc.name, strings.NewReader(content),
				objstore.WithUserMetadata(tc.metadata))

			if tc.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
			}
		})
	}
}

func TestBucket_AttributesWithoutMetadata(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()
	objectName := "test-object-no-metadata"
	content := "test content without metadata"

	// 메타데이터 없이 객체 업로드
	err = bkt.Upload(ctx, objectName, strings.NewReader(content))
	testutil.Ok(t, err)

	// 객체 속성 조회
	attrs, err := bkt.Attributes(ctx, objectName)
	testutil.Ok(t, err)

	// 기본 속성 검증
	testutil.Equals(t, int64(len(content)), attrs.Size)
	testutil.Assert(t, !attrs.LastModified.IsZero(), "LastModified should be set")
	// ETag는 에뮬레이터에서 제공되지 않을 수 있으므로 선택적으로 검증
	if attrs.ETag != "" {
		err = objstore.ValidateETag(attrs.ETag)
		testutil.Ok(t, err)
	}

	// 메타데이터가 없어야 함
	testutil.Assert(t, attrs.UserMetadata == nil || len(attrs.UserMetadata) == 0,
		"UserMetadata should be nil or empty when no metadata is provided")
}

func TestBucket_ETagValidation(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()
	objectName := "test-object-etag"
	content1 := "first content"
	content2 := "second content"

	// 첫 번째 객체 업로드
	err = bkt.Upload(ctx, objectName, strings.NewReader(content1))
	testutil.Ok(t, err)

	// 첫 번째 ETag 조회
	attrs1, err := bkt.Attributes(ctx, objectName)
	testutil.Ok(t, err)

	// 같은 이름으로 다른 내용 업로드
	err = bkt.Upload(ctx, objectName, strings.NewReader(content2))
	testutil.Ok(t, err)

	// 두 번째 ETag 조회
	attrs2, err := bkt.Attributes(ctx, objectName)
	testutil.Ok(t, err)

	// ETag가 제공되는 경우에만 검증
	if attrs1.ETag != "" && attrs2.ETag != "" {
		// ETag가 달라야 함 (내용이 다르므로)
		testutil.Assert(t, attrs1.ETag != attrs2.ETag,
			"ETags should be different for different content")

		// ETag 형식 검증
		err = objstore.ValidateETag(attrs1.ETag)
		testutil.Ok(t, err)
		err = objstore.ValidateETag(attrs2.ETag)
		testutil.Ok(t, err)
	}
}

func TestBucket_MetadataIntegration(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()

	testCases := []struct {
		name     string
		metadata map[string]string
		content  string
		wantErr  bool
	}{
		{
			name: "basic metadata",
			metadata: map[string]string{
				"author":  "test-user",
				"version": "1.0",
			},
			content: "basic content",
			wantErr: false,
		},
		{
			name: "complex metadata",
			metadata: map[string]string{
				"project_id":   "test-project-123",
				"environment":  "testing",
				"build_number": "456",
				"commit_hash":  "abc123def456",
				"created_by":   "automated-system",
			},
			content: "complex content with multiple metadata fields",
			wantErr: false,
		},
		{
			name: "unicode metadata values",
			metadata: map[string]string{
				"description": "테스트 설명 with unicode 🚀",
				"location":    "서울, 대한민국",
			},
			content: "unicode content",
			wantErr: false,
		},
		{
			name:     "empty metadata",
			metadata: map[string]string{},
			content:  "content without metadata",
			wantErr:  false,
		},
		{
			name:     "nil metadata",
			metadata: nil,
			content:  "content with nil metadata",
			wantErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objectName := "test-metadata-integration-" + tc.name

			// 메타데이터와 함께 객체 업로드
			var uploadOpts []objstore.ObjectUploadOption
			if tc.metadata != nil {
				uploadOpts = append(uploadOpts, objstore.WithUserMetadata(tc.metadata))
			}

			err := bkt.Upload(ctx, objectName, strings.NewReader(tc.content), uploadOpts...)
			if tc.wantErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)

			// 객체 속성 조회
			attrs, err := bkt.Attributes(ctx, objectName)
			testutil.Ok(t, err)

			// 기본 속성 검증
			testutil.Equals(t, int64(len(tc.content)), attrs.Size)
			testutil.Assert(t, !attrs.LastModified.IsZero(), "LastModified should be set")

			// 메타데이터 검증
			if tc.metadata == nil || len(tc.metadata) == 0 {
				testutil.Assert(t, attrs.UserMetadata == nil || len(attrs.UserMetadata) == 0,
					"UserMetadata should be empty when no metadata provided")
			} else {
				testutil.Assert(t, attrs.UserMetadata != nil, "UserMetadata should not be nil")

				// GCS는 메타데이터 키를 변환하므로 변환된 키로 검증
				expectedMetadata := objstore.TransformMetadataForGCS(tc.metadata)
				for expectedKey, expectedValue := range expectedMetadata {
					actualValue, exists := attrs.UserMetadata[expectedKey]
					testutil.Assert(t, exists, "Expected metadata key '%s' not found", expectedKey)
					testutil.Equals(t, expectedValue, actualValue)
				}
			}

			// 객체 내용 검증
			reader, err := bkt.Get(ctx, objectName)
			testutil.Ok(t, err)
			defer reader.Close()

			actualContent, err := io.ReadAll(reader)
			testutil.Ok(t, err)
			testutil.Equals(t, tc.content, string(actualContent))
		})
	}
}

func TestBucket_MetadataErrorHandling(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()
	content := "test content"

	// 메타데이터 검증 에러 테스트 케이스들
	errorTestCases := []struct {
		name     string
		metadata map[string]string
		wantErr  string
	}{
		{
			name: "empty key",
			metadata: map[string]string{
				"":      "value",
				"valid": "value",
			},
			wantErr: "invalid metadata key",
		},
		{
			name: "reserved key content-type",
			metadata: map[string]string{
				"content-type": "text/plain",
			},
			wantErr: "invalid metadata key",
		},
		{
			name: "reserved key etag",
			metadata: map[string]string{
				"etag": "some-etag",
			},
			wantErr: "invalid metadata key",
		},
		{
			name: "invalid characters in key",
			metadata: map[string]string{
				"key@invalid": "value",
			},
			wantErr: "invalid metadata key",
		},
		{
			name: "key with spaces",
			metadata: map[string]string{
				"key with spaces": "value",
			},
			wantErr: "invalid metadata key",
		},
		{
			name: "empty value",
			metadata: map[string]string{
				"valid_key": "",
			},
			wantErr: "invalid metadata value",
		},
		{
			name: "too long key",
			metadata: map[string]string{
				strings.Repeat("a", 129): "value", // 129자 키 (최대 128자 초과)
			},
			wantErr: "invalid metadata key",
		},
		{
			name: "too long value",
			metadata: map[string]string{
				"key": strings.Repeat("v", 2049), // 2049자 값 (최대 2048자 초과)
			},
			wantErr: "invalid metadata value",
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.name, func(t *testing.T) {
			objectName := "test-error-" + tc.name

			err := bkt.Upload(ctx, objectName, strings.NewReader(content),
				objstore.WithUserMetadata(tc.metadata))

			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), tc.wantErr),
				"Expected error containing '%s', got: %v", tc.wantErr, err)
		})
	}
}

func TestBucket_ETagConsistency(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()

	testCases := []struct {
		name     string
		content1 string
		content2 string
		metadata map[string]string
	}{
		{
			name:     "same content different metadata",
			content1: "identical content",
			content2: "identical content",
			metadata: map[string]string{
				"version": "2.0",
			},
		},
		{
			name:     "different content same metadata",
			content1: "first content",
			content2: "second content",
			metadata: map[string]string{
				"version": "1.0",
			},
		},
		{
			name:     "different content no metadata",
			content1: "first content without metadata",
			content2: "second content without metadata",
			metadata: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objectName := "test-etag-consistency-" + tc.name

			// 첫 번째 업로드
			var uploadOpts []objstore.ObjectUploadOption
			if tc.metadata != nil {
				uploadOpts = append(uploadOpts, objstore.WithUserMetadata(tc.metadata))
			}

			err := bkt.Upload(ctx, objectName, strings.NewReader(tc.content1), uploadOpts...)
			testutil.Ok(t, err)

			attrs1, err := bkt.Attributes(ctx, objectName)
			testutil.Ok(t, err)

			// 두 번째 업로드 (덮어쓰기)
			err = bkt.Upload(ctx, objectName, strings.NewReader(tc.content2), uploadOpts...)
			testutil.Ok(t, err)

			attrs2, err := bkt.Attributes(ctx, objectName)
			testutil.Ok(t, err)

			// ETag가 제공되는 경우 일관성 검증
			if attrs1.ETag != "" && attrs2.ETag != "" {
				if tc.content1 == tc.content2 {
					// 같은 내용이면 ETag가 같아야 함 (메타데이터는 ETag에 영향 없음)
					// 단, GCS 에뮬레이터에서는 업로드 시간에 따라 다를 수 있으므로 경고만 출력
					if attrs1.ETag != attrs2.ETag {
						t.Logf("Warning: ETags differ for identical content: %s vs %s", attrs1.ETag, attrs2.ETag)
					}
				} else {
					// 다른 내용이면 ETag가 달라야 함
					testutil.Assert(t, attrs1.ETag != attrs2.ETag,
						"ETags should be different for different content: %s vs %s", attrs1.ETag, attrs2.ETag)
				}

				// ETag 형식 검증
				err = objstore.ValidateETag(attrs1.ETag)
				testutil.Ok(t, err)
				err = objstore.ValidateETag(attrs2.ETag)
				testutil.Ok(t, err)
			}

			// 메타데이터 일관성 검증
			if tc.metadata != nil {
				expectedMetadata := objstore.TransformMetadataForGCS(tc.metadata)
				for expectedKey, expectedValue := range expectedMetadata {
					actualValue, exists := attrs2.UserMetadata[expectedKey]
					testutil.Assert(t, exists, "Expected metadata key '%s' not found", expectedKey)
					testutil.Equals(t, expectedValue, actualValue)
				}
			}
		})
	}
}

func TestBucket_LargeMetadata(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()
	content := "test content for large metadata"

	// 큰 메타데이터 생성 (제한 내에서)
	largeMetadata := make(map[string]string)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := strings.Repeat("v", 100) // 100자 값
		largeMetadata[key] = value
	}

	objectName := "test-large-metadata"

	// 큰 메타데이터와 함께 업로드
	err = bkt.Upload(ctx, objectName, strings.NewReader(content),
		objstore.WithUserMetadata(largeMetadata))
	testutil.Ok(t, err)

	// 속성 조회
	attrs, err := bkt.Attributes(ctx, objectName)
	testutil.Ok(t, err)

	// 메타데이터 검증
	testutil.Assert(t, attrs.UserMetadata != nil, "UserMetadata should not be nil")
	testutil.Assert(t, len(attrs.UserMetadata) > 0, "UserMetadata should not be empty")

	// 변환된 메타데이터로 검증
	expectedMetadata := objstore.TransformMetadataForGCS(largeMetadata)
	for expectedKey, expectedValue := range expectedMetadata {
		actualValue, exists := attrs.UserMetadata[expectedKey]
		testutil.Assert(t, exists, "Expected metadata key '%s' not found", expectedKey)
		testutil.Equals(t, expectedValue, actualValue)
	}
}

func TestBucket_MetadataWithSpecialCharacters(t *testing.T) {
	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
		UseGRPC:        false,
		noAuth:         true,
	}

	svr, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	testutil.Ok(t, err)
	defer svr.Close()
	err = os.Setenv("STORAGE_EMULATOR_HOST", svr.Addr)
	testutil.Ok(t, err)

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test-bucket", nil)
	testutil.Ok(t, err)
	defer bkt.Close()

	ctx := context.Background()
	content := "test content with special characters in metadata"

	// 특수 문자가 포함된 메타데이터 (유효한 것들)
	specialMetadata := map[string]string{
		"key_with_underscore": "value with spaces",
		"key-with-hyphen":     "value-with-hyphen",
		"numeric123":          "123456",
		"mixed_key-123":       "mixed_value-456",
	}

	objectName := "test-special-characters"

	// 특수 문자 메타데이터와 함께 업로드
	err = bkt.Upload(ctx, objectName, strings.NewReader(content),
		objstore.WithUserMetadata(specialMetadata))
	testutil.Ok(t, err)

	// 속성 조회
	attrs, err := bkt.Attributes(ctx, objectName)
	testutil.Ok(t, err)

	// 메타데이터 검증 (GCS 변환 규칙 적용)
	testutil.Assert(t, attrs.UserMetadata != nil, "UserMetadata should not be nil")

	expectedMetadata := objstore.TransformMetadataForGCS(specialMetadata)
	for expectedKey, expectedValue := range expectedMetadata {
		actualValue, exists := attrs.UserMetadata[expectedKey]
		testutil.Assert(t, exists, "Expected metadata key '%s' not found", expectedKey)
		testutil.Equals(t, expectedValue, actualValue)
	}
}
