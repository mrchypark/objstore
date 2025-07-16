// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filesystem

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/efficientgo/core/errcapture"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore"
)

// Config stores the configuration for storing and accessing blobs in filesystem.
type Config struct {
	Directory string `yaml:"directory"`
}

// Bucket implements the objstore.Bucket interfaces against filesystem that binary runs on.
// Methods from Bucket interface are thread-safe. Objects are assumed to be immutable.
// NOTE: It does not follow symbolic links.
type Bucket struct {
	rootDir string
}

// NewBucketFromConfig returns a new filesystem.Bucket from config.
func NewBucketFromConfig(conf []byte) (*Bucket, error) {
	var c Config
	if err := yaml.Unmarshal(conf, &c); err != nil {
		return nil, err
	}
	if c.Directory == "" {
		return nil, errors.New("missing directory for filesystem bucket")
	}
	return NewBucket(c.Directory)
}

// NewBucket returns a new filesystem.Bucket.
func NewBucket(rootDir string) (*Bucket, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	return &Bucket{rootDir: absDir}, nil
}

func (b *Bucket) Provider() objstore.ObjProvider { return objstore.FILESYSTEM }

func (b *Bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt}
}

func (b *Bucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}

	params := objstore.ApplyIterOptions(options...)
	absDir := filepath.Join(b.rootDir, dir)
	info, err := os.Stat(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "stat %s", absDir)
	}
	if !info.IsDir() {
		return nil
	}

	files, err := os.ReadDir(absDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		name := filepath.Join(dir, file.Name())

		if file.IsDir() {
			empty, err := isDirEmpty(filepath.Join(absDir, file.Name()))
			if err != nil {
				return err
			}

			if empty {
				// Skip empty directories.
				continue
			}

			name += objstore.DirDelim

			if params.Recursive {
				// Recursively list files in the subdirectory.
				if err := b.IterWithAttributes(ctx, name, f, options...); err != nil {
					return err
				}

				// The callback f() has already been called for the subdirectory
				// files so we should skip to next filesystem entry.
				continue
			}
		}

		attrs := objstore.IterObjectAttributes{
			Name: name,
		}
		if params.LastModified {
			absPath := filepath.Join(absDir, file.Name())
			stat, err := os.Stat(absPath)
			if err != nil {
				return errors.Wrapf(err, "stat %s", name)
			}
			attrs.SetLastModified(stat.ModTime())
		}
		if err := f(attrs); err != nil {
			return err
		}
	}
	return nil
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	// Only include recursive option since attributes are not used in this method.
	var filteredOpts []objstore.IterOption
	for _, opt := range opts {
		if opt.Type == objstore.Recursive {
			filteredOpts = append(filteredOpts, opt)
			break
		}
	}

	return b.IterWithAttributes(ctx, dir, func(attrs objstore.IterObjectAttributes) error {
		return f(attrs.Name)
	}, filteredOpts...)
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.GetRange(ctx, name, 0, -1)
}

type rangeReaderCloser struct {
	io.Reader
	f *os.File
}

func (r *rangeReaderCloser) Close() error {
	return r.f.Close()
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if ctx.Err() != nil {
		return objstore.ObjectAttributes{}, ctx.Err()
	}

	file := filepath.Join(b.rootDir, name)
	stat, err := os.Stat(file)
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrapf(err, "stat %s", file)
	}

	// 메타데이터 로드
	metadata, err := b.loadMetadata(name)
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrapf(err, "load metadata for %s", name)
	}

	// ETag가 없으면 파일 콘텐츠를 기반으로 생성
	etag := metadata.ETag
	if etag == "" {
		// 파일을 읽어서 ETag 생성
		f, err := os.Open(file)
		if err != nil {
			return objstore.ObjectAttributes{}, errors.Wrapf(err, "open file %s for ETag generation", name)
		}
		defer f.Close()

		generatedETag, err := objstore.GenerateContentETag(f)
		if err != nil {
			return objstore.ObjectAttributes{}, errors.Wrapf(err, "generate ETag for %s", name)
		}
		etag = generatedETag

		// 생성된 ETag를 메타데이터에 저장
		metadata.ETag = etag
		metadata.UpdatedAt = time.Now()
		if err := b.saveMetadata(name, metadata); err != nil {
			// ETag 저장 실패는 로그만 남기고 계속 진행
			// 실제 구현에서는 로거를 사용해야 함
		}
	}

	return objstore.ObjectAttributes{
		Size:         stat.Size(),
		LastModified: stat.ModTime(),
		ETag:         etag,
		UserMetadata: metadata.UserMetadata,
	}, nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if name == "" {
		return nil, errors.New("object name is empty")
	}

	var (
		file = filepath.Join(b.rootDir, name)
		stat os.FileInfo
		err  error
	)
	if stat, err = os.Stat(file); err != nil {
		return nil, errors.Wrapf(err, "stat %s", file)
	}

	f, err := os.OpenFile(filepath.Clean(file), os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}

	var newOffset int64
	if off > 0 {
		newOffset, err = f.Seek(off, 0)
		if err != nil {
			return nil, errors.Wrapf(err, "seek %v", off)
		}
	}

	size := stat.Size() - newOffset
	if length == -1 {
		return objstore.ObjectSizerReadCloser{
			ReadCloser: f,
			Size: func() (int64, error) {
				return size, nil
			},
		}, nil
	}

	return objstore.ObjectSizerReadCloser{
		ReadCloser: &rangeReaderCloser{
			Reader: io.LimitReader(f, length),
			f:      f,
		},
		Size: func() (int64, error) {
			return min(length, size), nil
		},
	}, nil
}

// Exists checks if the given directory exists in memory.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	info, err := os.Stat(filepath.Join(b.rootDir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "stat %s", filepath.Join(b.rootDir, name))
	}
	return !info.IsDir(), nil
}

// Upload writes the file specified in src to into the filesystem with metadata support.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 업로드 옵션 파싱
	params := objstore.ApplyUploadOptions(opts...)

	// 사용자 메타데이터 검증
	if err := objstore.ValidateUserMetadata(params.UserMetadata); err != nil {
		return errors.Wrapf(err, "invalid metadata for object %s", name)
	}

	file := filepath.Join(b.rootDir, name)
	if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
		return err
	}

	// 임시 파일 생성 (원자적 쓰기를 위해)
	tempFile := file + ".tmp"
	f, err := os.Create(tempFile)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(tempFile) // 실패 시 임시 파일 정리
		}
	}()

	// 콘텐츠를 임시 파일에 쓰면서 ETag 계산을 위한 해시 생성
	hasher := objstore.NewContentHasher()
	multiWriter := io.MultiWriter(f, hasher)

	if _, err := io.Copy(multiWriter, r); err != nil {
		return errors.Wrapf(err, "copy to %s", file)
	}

	// 파일 동기화
	if err := f.Sync(); err != nil {
		return errors.Wrapf(err, "sync file %s", file)
	}

	// 파일 닫기
	if err := f.Close(); err != nil {
		return errors.Wrapf(err, "close file %s", file)
	}

	// ETag 생성
	etag := hasher.Sum()

	// 메타데이터 생성
	now := time.Now()
	metadata := &ObjectMetadata{
		UserMetadata: params.UserMetadata,
		ETag:         etag,
		ContentType:  params.ContentType,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// 메타데이터 저장 (원자적 쓰기)
	if err := b.saveMetadata(name, metadata); err != nil {
		os.Remove(tempFile) // 메타데이터 저장 실패 시 임시 파일 정리
		return errors.Wrapf(err, "save metadata for %s", name)
	}

	// 원자적 이동 (rename)
	if err := os.Rename(tempFile, file); err != nil {
		// 파일 이동 실패 시 메타데이터도 정리
		b.deleteMetadata(name)
		return errors.Wrapf(err, "rename file %s", name)
	}

	return nil
}

func isDirEmpty(name string) (ok bool, err error) {
	f, err := os.Open(filepath.Clean(name))
	if os.IsNotExist(err) {
		// The directory doesn't exist. We don't consider it an error and we treat it like empty.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer errcapture.Do(&err, f.Close, "close dir")

	if _, err = f.Readdir(1); err == io.EOF || os.IsNotExist(err) {
		return true, nil
	}
	return false, err
}

// Delete removes all data prefixed with the dir.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 메타데이터 파일도 함께 삭제
	if err := b.deleteMetadata(name); err != nil {
		// 메타데이터 삭제 실패는 로그만 남기고 계속 진행
		// 실제 구현에서는 로거를 사용해야 함
	}

	file := filepath.Join(b.rootDir, name)
	for file != b.rootDir {
		if err := os.RemoveAll(file); err != nil {
			return errors.Wrapf(err, "rm %s", file)
		}
		file = filepath.Dir(file)
		empty, err := isDirEmpty(file)
		if err != nil {
			return err
		}
		if !empty {
			break
		}
	}
	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return os.IsNotExist(errors.Cause(err))
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *Bucket) IsAccessDeniedErr(_ error) bool {
	return false
}

func (b *Bucket) Close() error { return nil }

// Name returns the bucket name.
func (b *Bucket) Name() string {
	return fmt.Sprintf("fs: %s", b.rootDir)
}

// ObjectMetadata represents metadata stored for filesystem objects.
// 파일시스템 객체에 대해 저장되는 메타데이터를 나타냅니다.
type ObjectMetadata struct {
	UserMetadata map[string]string `json:"user_metadata"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"content_type,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// getMetadataPath returns the path to the metadata file for a given object.
// 주어진 객체에 대한 메타데이터 파일의 경로를 반환합니다.
func (b *Bucket) getMetadataPath(name string) string {
	return filepath.Join(b.rootDir, name+".metadata")
}

// saveMetadata saves metadata to a JSON file atomically using a temporary file.
// 임시 파일을 사용하여 메타데이터를 JSON 파일에 원자적으로 저장합니다.
func (b *Bucket) saveMetadata(name string, metadata *ObjectMetadata) error {
	metadataPath := b.getMetadataPath(name)

	// 메타데이터 디렉토리 생성
	if err := os.MkdirAll(filepath.Dir(metadataPath), os.ModePerm); err != nil {
		return errors.Wrapf(err, "create metadata directory for %s", name)
	}

	// 임시 파일 생성 (원자적 쓰기를 위해)
	tempPath := metadataPath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return errors.Wrapf(err, "create temporary metadata file for %s", name)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempPath) // 실패 시 임시 파일 정리
	}()

	// JSON 인코딩 및 쓰기
	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		return errors.Wrapf(err, "encode metadata for %s", name)
	}

	// 파일 동기화
	if err := tempFile.Sync(); err != nil {
		return errors.Wrapf(err, "sync metadata file for %s", name)
	}

	// 파일 닫기
	if err := tempFile.Close(); err != nil {
		return errors.Wrapf(err, "close metadata file for %s", name)
	}

	// 원자적 이동 (rename)
	if err := os.Rename(tempPath, metadataPath); err != nil {
		return errors.Wrapf(err, "rename metadata file for %s", name)
	}

	return nil
}

// loadMetadata loads metadata from a JSON file.
// JSON 파일에서 메타데이터를 로드합니다.
func (b *Bucket) loadMetadata(name string) (*ObjectMetadata, error) {
	metadataPath := b.getMetadataPath(name)

	file, err := os.Open(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			// 메타데이터 파일이 없으면 기본값 반환
			return &ObjectMetadata{
				UserMetadata: make(map[string]string),
				ETag:         "",
				CreatedAt:    time.Now(),
				UpdatedAt:    time.Now(),
			}, nil
		}
		return nil, errors.Wrapf(err, "open metadata file for %s", name)
	}
	defer file.Close()

	var metadata ObjectMetadata
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, errors.Wrapf(err, "decode metadata for %s", name)
	}

	// nil 맵 초기화
	if metadata.UserMetadata == nil {
		metadata.UserMetadata = make(map[string]string)
	}

	return &metadata, nil
}

// deleteMetadata removes the metadata file for an object.
// 객체의 메타데이터 파일을 제거합니다.
func (b *Bucket) deleteMetadata(name string) error {
	metadataPath := b.getMetadataPath(name)

	err := os.Remove(metadataPath)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove metadata file for %s", name)
	}

	return nil
}
