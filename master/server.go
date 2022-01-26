package master

import (
	"context"

	orch "github.com/engula/shared-storage/master/orchestrator"
	masterpb "github.com/engula/shared-storage/proto/master"
	serverpb "github.com/engula/shared-storage/proto/server"
	"github.com/engula/shared-storage/server/storage"
	"google.golang.org/grpc"
)

var (
	_ masterpb.BucketServiceServer   = &Server{}
	_ masterpb.ObjectPlacementServer = &Server{}
)

type Server struct {
	baseStorage  storage.Storage
	orchestrator orch.Orchestrator
}

func NewMasterServer(base storage.Storage, orch orch.Orchestrator) *Server {
	return &Server{baseStorage: base, orchestrator: orch}
}

func (s *Server) CreateBucket(ctx context.Context, req *masterpb.CreateBucketRequest) (resp *masterpb.CreateBucketResponse, err error) {
	if err = s.baseStorage.CreateBucket(ctx, req.Bucket); err != nil {
		return
	}

	if err = orch.RunWithAllCacheNodes(ctx, s.orchestrator, func(ctx context.Context, conn *grpc.ClientConn) (err error) {
		c := serverpb.NewBucketServiceClient(conn)
		_, err = c.CreateBucket(ctx, &serverpb.CreateBucketRequest{Bucket: req.Bucket})
		return
	}); err != nil {
		return
	}
	return
}

func (s *Server) DeleteBucket(ctx context.Context, req *masterpb.DeleteBucketRequest) (resp *masterpb.DeleteBucketResponse, err error) {
	if err = s.baseStorage.CreateBucket(ctx, req.Bucket); err != nil {
		return
	}

	if err = orch.RunWithAllCacheNodes(ctx, s.orchestrator, func(ctx context.Context, conn *grpc.ClientConn) (err error) {
		c := serverpb.NewBucketServiceClient(conn)
		_, err = c.DeleteBucket(ctx, &serverpb.DeleteBucketRequest{Bucket: req.Bucket})
		return
	}); err != nil {
		return
	}
	return
}

func (s *Server) ListBuckets(ctx context.Context, req *masterpb.ListBucketsRequest) (resp *masterpb.ListBucketsResponse, err error) {
	var buckets []string
	if buckets, err = s.baseStorage.ListBuckets(ctx); err != nil {
		return
	}
	resp = &masterpb.ListBucketsResponse{Buckets: buckets}
	return
}

func (s *Server) ListObjects(ctx context.Context, req *masterpb.ListObjectsRequest) (resp *masterpb.ListObjectsResponse, err error) {
	var objects []string
	if objects, err = s.baseStorage.ListObjects(ctx, req.Bucket); err != nil {
		return
	}
	resp = &masterpb.ListObjectsResponse{Objects: objects}
	return
}

func (s *Server) PlaceLookup(ctx context.Context, req *masterpb.PlaceLookupRequest) (resp *masterpb.PlaceLookupResponse, error error) {
	panic("todo.")
}
