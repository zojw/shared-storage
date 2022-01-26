package storage

import (
	"context"
)

const (
	objectCategory = "buckets"
	streamCategory = "streams"
)

type Storage interface {
	CreateBucket(ctx context.Context, bucket string) (err error)
	DeleteBucket(ctx context.Context, bucket string) (err error)
	ListBuckets(ctx context.Context) (buckets []string, err error)
	DeleteObject(ctx context.Context, bucket, object string) (err error)
	ListObjects(ctx context.Context, bucket string) (objects []string, err error)
	ReadObject(ctx context.Context, bucket, object string, pos, len int32) (bytes []byte, err error)
	PutObject(ctx context.Context, bucket, object string) (writer ObjectWriter, err error)
}

type ObjectWriter interface {
	Write(ctx context.Context, p []byte) (err error)
	Finish(ctx context.Context) (err error)
}
