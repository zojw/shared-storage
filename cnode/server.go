package cnode

import (
	"context"

	"github.com/engula/shared-storage/cnode/storage"
	cnode "github.com/engula/shared-storage/proto"
)

var (
	_ cnode.StorageServer = &Server{}
)

type Server struct {
	base    storage.Storage
	local   storage.Storage
	replica storage.Storage
}

func NewServer(base, local, replica storage.Storage) *Server {
	return &Server{
		base:    base,
		local:   local,
		replica: replica,
	}
}

func (s *Server) CreateBucket(ctx context.Context, req *cnode.CreateBucketRequest) (resp *cnode.CreateBucketResponse, err error) {
	if err = s.local.CreateBucket(ctx, req.Bucket); err != nil {
		return
	}
	resp = &cnode.CreateBucketResponse{}
	return
}
func (s *Server) DeleteBucket(ctx context.Context, req *cnode.DeleteBucketRequest) (resp *cnode.DeleteBucketResponse, err error) {
	if err = s.local.DeleteBucket(ctx, req.Bucket); err != nil {
		return
	}
	resp = &cnode.DeleteBucketResponse{}
	return
}
func (s *Server) ListBuckets(ctx context.Context, _ *cnode.ListBucketsRequest) (resp *cnode.ListBucketsResponse, err error) {
	var buckets []string
	if buckets, err = s.local.ListBuckets(ctx); err != nil {
		return
	}
	resp = &cnode.ListBucketsResponse{Buckets: buckets}
	return
}

func (s *Server) DeleteObject(ctx context.Context, req *cnode.DeleteObjectRequest) (resp *cnode.DeleteObjectResponse, err error) {
	if err = s.local.DeleteObject(ctx, req.Bucket, req.Object); err != nil {
		return
	}
	resp = &cnode.DeleteObjectResponse{}
	return
}
func (s *Server) ListObjects(ctx context.Context, req *cnode.ListObjectsRequest) (resp *cnode.ListObjectsResponse, err error) {
	var objects []string
	if objects, err = s.local.ListObjects(ctx, req.Bucket); err != nil {
		return
	}
	resp = &cnode.ListObjectsResponse{Objects: objects}
	return
}
func (s *Server) ReadObject(ctx context.Context, req *cnode.ReadObjectRequest) (resp *cnode.ReadObjectResponse, err error) {
	var result []byte
	if result, err = s.local.ReadObject(ctx, req.Bucket, req.Object, req.Pos, req.Len); err != nil {
		return
	}
	resp = &cnode.ReadObjectResponse{Content: result}
	return
}

func (s *Server) UploadObject(srv cnode.Storage_UploadObjectServer) (err error) {
	var first *cnode.UploadObjectRequest
	if first, err = srv.Recv(); err != nil {
		return
	}

	var uploader storage.ObjectUploader
	if uploader, err = s.local.UploadObject(srv.Context(), first.Bucket, first.Object); err != nil {
		return
	}
	uploader.UploadCh() <- first.Content

	defer func() {
		close(uploader.UploadCh())
	}()
	var req *cnode.UploadObjectRequest
	for {
		select {
		case err = <-uploader.Done():
			return
		default:
		}
		if req, err = srv.Recv(); err != nil {
			return
		}
		if req == nil {
			break
		}
		uploader.UploadCh() <- req.Content
	}

	srv.SendAndClose(&cnode.UploadObjectResponse{})
	return
}
