package storage

import (
	"context"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
)

var (
	_ Storage = &localFileStorage{}
)

type localFileStorage struct {
	rootPath string
}

func NewLocalFile(ctx context.Context, rootPath string) (s Storage, err error) {
	s = &localFileStorage{rootPath: filepath.Join(rootPath, objectCategory)}
	return
}

func (s *localFileStorage) bucketPath(bucket string) string {
	return filepath.Join(s.rootPath, bucket)
}

func (s *localFileStorage) objectPath(bucket, object string) string {
	return filepath.Join(s.rootPath, bucket, object)
}

func (s *localFileStorage) CreateBucket(ctx context.Context, bucket string) (err error) {
	err = os.MkdirAll(s.bucketPath(bucket), os.ModePerm)
	return
}

func (s *localFileStorage) DeleteBucket(ctx context.Context, bucket string) (err error) {
	err = os.RemoveAll(s.bucketPath(bucket))
	return
}

func (s *localFileStorage) ListBuckets(ctx context.Context) (buckets []string, err error) {
	var files []fs.FileInfo
	if files, err = ioutil.ReadDir(s.rootPath); err != nil {
		return
	}
	buckets = make([]string, 0, len(files))
	for _, f := range files {
		buckets = append(buckets, f.Name())
	}
	return
}

func (s *localFileStorage) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	err = os.Remove(s.objectPath(bucket, object))
	return
}

func (s *localFileStorage) ListObjects(ctx context.Context, bucket string) (objects []string, err error) {
	var files []fs.FileInfo
	if files, err = ioutil.ReadDir(s.bucketPath(bucket)); err != nil {
		return
	}
	objects = make([]string, 0, len(files))
	for _, f := range files {
		objects = append(objects, f.Name())
	}
	return
}

func (s *localFileStorage) ReadObject(ctx context.Context, bucket, object string, pos, len int32) (bytes []byte, err error) {
	var f *os.File
	if f, err = os.OpenFile(s.objectPath(bucket, object), os.O_RDONLY, 0666); err != nil {
		return
	}
	var n int
	bytes = make([]byte, len)
	if n, err = f.ReadAt(bytes, int64(pos)); err != nil {
		return
	}
	bytes = bytes[:n]
	return
}

func (s *localFileStorage) PutObject(ctx context.Context, bucket, object string) (writer ObjectWriter, err error) {
	var f *os.File
	if f, err = os.OpenFile(s.objectPath(bucket, object), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666); err != nil {
		return
	}
	writer = &localWriter{file: f}
	return
}

var (
	_ ObjectWriter = &localWriter{}
)

type localWriter struct {
	file *os.File
}

func (w *localWriter) Write(ctx context.Context, p []byte) (err error) {
	_, err = w.file.Write(p)
	return
}

func (w *localWriter) Finish(ctx context.Context) (err error) {
	if err = w.file.Sync(); err != nil {
		return
	}
	err = w.file.Close()
	return
}
