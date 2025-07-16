// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filesystem

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/objstore"
)

func TestDelete_EmptyDirDeletionRaceCondition(t *testing.T) {
	const runs = 1000

	ctx := context.Background()

	for r := 0; r < runs; r++ {
		b, err := NewBucket(t.TempDir())
		testutil.Ok(t, err)

		// Upload 2 objects in a subfolder.
		testutil.Ok(t, b.Upload(ctx, "subfolder/first", strings.NewReader("first")))
		testutil.Ok(t, b.Upload(ctx, "subfolder/second", strings.NewReader("second")))

		// Prepare goroutines to concurrently delete the 2 objects (each one deletes a different object)
		start := make(chan struct{})
		group := sync.WaitGroup{}
		group.Add(2)

		for _, object := range []string{"first", "second"} {
			go func(object string) {
				defer group.Done()

				<-start
				testutil.Ok(t, b.Delete(ctx, "subfolder/"+object))
			}(object)
		}

		// Go!
		close(start)
		group.Wait()
	}
}

func TestIter_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Iter(ctx, "", func(s string) error {
		return nil
	})

	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestIterWithAttributes(t *testing.T) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "test")
	testutil.Ok(t, err)
	defer f.Close()

	stat, err := f.Stat()
	testutil.Ok(t, err)

	cases := []struct {
		name              string
		opts              []objstore.IterOption
		expectedUpdatedAt time.Time
	}{
		{
			name: "no options",
			opts: nil,
		},
		{
			name: "with updated at",
			opts: []objstore.IterOption{
				objstore.WithUpdatedAt(),
			},
			expectedUpdatedAt: stat.ModTime(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBucket(dir)
			testutil.Ok(t, err)

			var attrs objstore.IterObjectAttributes

			ctx := context.Background()
			err = b.IterWithAttributes(ctx, "", func(objectAttrs objstore.IterObjectAttributes) error {
				attrs = objectAttrs
				return nil
			}, tc.opts...)

			testutil.Ok(t, err)

			lastModified, ok := attrs.LastModified()
			if zero := tc.expectedUpdatedAt.IsZero(); zero {
				testutil.Equals(t, false, ok)
			} else {
				testutil.Equals(t, true, ok)
				testutil.Equals(t, tc.expectedUpdatedAt, lastModified)
			}
		})

	}
}

func TestGet_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Get(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestAttributes_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Attributes(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestGetRange_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.GetRange(ctx, "some-file", 0, 100)
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestExists_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.Exists(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestUpload_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Upload(ctx, "some-file", bytes.NewReader([]byte("file content")))
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestDelete_CancelledContext(t *testing.T) {
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.Delete(ctx, "some-file")
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
}

func TestMetadataFileOperations(t *testing.T) {
	ctx := context.Background()
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	// 테스트 메타데이터
	testMetadata := map[string]string{
		"author":      "test-user",
		"version":     "1.0",
		"environment": "test",
	}

	// 메타데이터와 함께 파일 업로드
	content := "test file content for metadata"
	err = b.Upload(ctx, "test-file", strings.NewReader(content),
		objstore.WithUserMetadata(testMetadata),
		objstore.WithContentType("text/plain"))
	testutil.Ok(t, err)

	// 메타데이터 파일이 생성되었는지 확인
	metadataPath := b.getMetadataPath("test-file")
	_, err = os.Stat(metadataPath)
	testutil.Ok(t, err)

	// 메타데이터 로드 테스트
	metadata, err := b.loadMetadata("test-file")
	testutil.Ok(t, err)
	testutil.Equals(t, testMetadata["author"], metadata.UserMetadata["author"])
	testutil.Equals(t, testMetadata["version"], metadata.UserMetadata["version"])
	testutil.Equals(t, testMetadata["environment"], metadata.UserMetadata["environment"])
	testutil.Equals(t, "text/plain", metadata.ContentType)
	testutil.Assert(t, metadata.ETag != "", "ETag should not be empty")
	testutil.Assert(t, strings.HasPrefix(metadata.ETag, "sha256:"), "ETag should have sha256 prefix")

	// Attributes를 통한 메타데이터 조회 테스트
	attrs, err := b.Attributes(ctx, "test-file")
	testutil.Ok(t, err)
	testutil.Equals(t, int64(len(content)), attrs.Size)
	testutil.Equals(t, testMetadata["author"], attrs.UserMetadata["author"])
	testutil.Equals(t, testMetadata["version"], attrs.UserMetadata["version"])
	testutil.Equals(t, testMetadata["environment"], attrs.UserMetadata["environment"])
	testutil.Assert(t, attrs.ETag != "", "ETag should not be empty")
	testutil.Assert(t, strings.HasPrefix(attrs.ETag, "sha256:"), "ETag should have sha256 prefix")

	// 파일 삭제 시 메타데이터 파일도 삭제되는지 확인
	err = b.Delete(ctx, "test-file")
	testutil.Ok(t, err)

	// 메타데이터 파일이 삭제되었는지 확인
	_, err = os.Stat(metadataPath)
	testutil.Assert(t, os.IsNotExist(err), "metadata file should be deleted")
}

func TestETagGeneration(t *testing.T) {
	ctx := context.Background()
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	// 동일한 콘텐츠로 두 번 업로드
	content := "test content for etag generation"

	// 첫 번째 업로드
	err = b.Upload(ctx, "test-file-1", strings.NewReader(content))
	testutil.Ok(t, err)

	// 두 번째 업로드 (동일한 콘텐츠)
	err = b.Upload(ctx, "test-file-2", strings.NewReader(content))
	testutil.Ok(t, err)

	// 두 파일의 ETag가 동일한지 확인
	attrs1, err := b.Attributes(ctx, "test-file-1")
	testutil.Ok(t, err)

	attrs2, err := b.Attributes(ctx, "test-file-2")
	testutil.Ok(t, err)

	testutil.Equals(t, attrs1.ETag, attrs2.ETag)
	testutil.Assert(t, strings.HasPrefix(attrs1.ETag, "sha256:"), "ETag should have sha256 prefix")

	// 다른 콘텐츠로 업로드
	differentContent := "different content"
	err = b.Upload(ctx, "test-file-3", strings.NewReader(differentContent))
	testutil.Ok(t, err)

	attrs3, err := b.Attributes(ctx, "test-file-3")
	testutil.Ok(t, err)

	// ETag가 다른지 확인
	testutil.Assert(t, attrs1.ETag != attrs3.ETag, "ETags should be different for different content")
}

func TestETagConsistency(t *testing.T) {
	ctx := context.Background()
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	content := "test content for etag consistency"

	// 파일 업로드
	err = b.Upload(ctx, "test-file", strings.NewReader(content))
	testutil.Ok(t, err)

	// 여러 번 Attributes 호출하여 ETag 일관성 확인
	var etags []string
	for i := 0; i < 5; i++ {
		attrs, err := b.Attributes(ctx, "test-file")
		testutil.Ok(t, err)
		etags = append(etags, attrs.ETag)
	}

	// 모든 ETag가 동일한지 확인
	for i := 1; i < len(etags); i++ {
		testutil.Equals(t, etags[0], etags[i])
	}
}

func TestMetadataFileErrorHandling(t *testing.T) {
	ctx := context.Background()
	b, err := NewBucket(t.TempDir())
	testutil.Ok(t, err)

	// 존재하지 않는 파일의 메타데이터 로드 시 기본값 반환 확인
	metadata, err := b.loadMetadata("non-existent-file")
	testutil.Ok(t, err)
	testutil.Assert(t, metadata.UserMetadata != nil, "UserMetadata should not be nil")
	testutil.Equals(t, "", metadata.ETag)

	// 파일은 있지만 메타데이터 파일이 없는 경우
	content := "test content without metadata"
	err = b.Upload(ctx, "test-file", strings.NewReader(content))
	testutil.Ok(t, err)

	// 메타데이터 파일 삭제 (시뮬레이션)
	metadataPath := b.getMetadataPath("test-file")
	err = os.Remove(metadataPath)
	testutil.Ok(t, err)

	// Attributes 호출 시 ETag가 자동 생성되는지 확인
	attrs, err := b.Attributes(ctx, "test-file")
	testutil.Ok(t, err)
	testutil.Assert(t, attrs.ETag != "", "ETag should be generated automatically")
	testutil.Assert(t, strings.HasPrefix(attrs.ETag, "sha256:"), "ETag should have sha256 prefix")

	// 메타데이터 파일이 다시 생성되었는지 확인
	_, err = os.Stat(metadataPath)
	testutil.Ok(t, err)
}
