package storage

import (
	"context"

	cnode "github.com/engula/shared-storage/proto"
)

type Storage interface {
	CreateBucket(ctx context.Context, bucket string) (err error)
	DeleteBucket(ctx context.Context, bucket string) (err error)
	ListBuckets(ctx context.Context) (buckets []string, err error)
	UploadObject(upload ObjectUpload) (err error)
	DeleteObject(ctx context.Context, bucket, object string) (err error)
	ListObjects(ctx context.Context, bucket string) (objects []string, err error)
	ReadObject(ctx context.Context, bucket, object string, pos, len int32) (bytes []byte, err error)
}

type ObjectUpload interface {
	Context() context.Context
	SendAndClose(resp *cnode.UploadObjectResponse) (err error)
	Recv() (req *cnode.UploadObjectRequest, err error)
}
