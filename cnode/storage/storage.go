package storage

import (
	"context"
)

type Storage interface {
	CreateBucket(ctx context.Context, bucket string) (err error)
	DeleteBucket(ctx context.Context, bucket string) (err error)
	ListBuckets(ctx context.Context) (buckets []string, err error)
	DeleteObject(ctx context.Context, bucket, object string) (err error)
	ListObjects(ctx context.Context, bucket string) (objects []string, err error)
	ReadObject(ctx context.Context, bucket, object string, pos, len int32) (bytes []byte, err error)

	UploadObject(ctx context.Context, bucket, object string) (uploader ObjectUploader, err error)
}

type ObjectUploader interface {
	UploadCh() chan<- []byte
	Done() <-chan error
}
