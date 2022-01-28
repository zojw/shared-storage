package server

import (
	"context"

	"github.com/engula/shared-storage/proto/server"
	"github.com/engula/shared-storage/server/storage"
)

var (
	_ server.ObjectServiceServer    = &Server{}
	_ server.BucketServiceServer    = &Server{}
	_ server.CacheNodeServiceServer = &Server{}
)

type Server struct {
	id      uint32
	base    storage.Storage
	local   storage.Storage
	replica storage.Storage
	status  *Status
}

func NewServer(serverId uint32, base, local, replica storage.Storage, status *Status) *Server {
	return &Server{
		id:      serverId,
		base:    base,
		local:   local,
		replica: replica,
		status:  status,
	}
}

func (s *Server) CreateBucket(ctx context.Context, req *server.CreateBucketRequest) (resp *server.CreateBucketResponse, err error) {
	if err = s.local.CreateBucket(ctx, req.Bucket); err != nil {
		return
	}
	s.status.AddBucket(ctx, req.Bucket)
	resp = &server.CreateBucketResponse{}
	return
}
func (s *Server) DeleteBucket(ctx context.Context, req *server.DeleteBucketRequest) (resp *server.DeleteBucketResponse, err error) {
	if err = s.local.DeleteBucket(ctx, req.Bucket); err != nil {
		return
	}
	s.status.DeleteBucket(ctx, req.Bucket)
	resp = &server.DeleteBucketResponse{}
	return
}
func (s *Server) ListBuckets(ctx context.Context, _ *server.ListBucketsRequest) (resp *server.ListBucketsResponse, err error) {
	var buckets []string
	if buckets, err = s.local.ListBuckets(ctx); err != nil {
		return
	}
	resp = &server.ListBucketsResponse{Buckets: buckets}
	return
}

func (s *Server) DeleteObject(ctx context.Context, req *server.DeleteObjectRequest) (resp *server.DeleteObjectResponse, err error) {
	if err = s.local.DeleteObject(ctx, req.Bucket, req.Object); err != nil {
		return
	}
	s.status.DeleteObject(ctx, req.Bucket, req.Object)
	resp = &server.DeleteObjectResponse{}
	return
}
func (s *Server) ListObjects(ctx context.Context, req *server.ListObjectsRequest) (resp *server.ListObjectsResponse, err error) {
	var objects []string
	if objects, err = s.local.ListObjects(ctx, req.Bucket); err != nil {
		return
	}
	resp = &server.ListObjectsResponse{Objects: objects}
	return
}
func (s *Server) ReadObject(ctx context.Context, req *server.ReadObjectRequest) (resp *server.ReadObjectResponse, err error) {
	var result []byte
	if result, err = s.local.ReadObject(ctx, req.Bucket, req.Object, req.Pos, req.Len); err != nil {
		return
	}
	resp = &server.ReadObjectResponse{Content: result}
	return
}

func (s *Server) UploadObject(srv server.ObjectService_UploadObjectServer) (err error) {
	var first *server.UploadObjectRequest
	if first, err = srv.Recv(); err != nil {
		return
	}
	if first == nil {
		srv.SendAndClose(&server.UploadObjectResponse{})
		return
	}

	ctx := srv.Context()

	var localWriter, baseWriter storage.ObjectWriter
	if localWriter, err = s.local.PutObject(ctx, first.Bucket, first.Object); err != nil {
		return
	}
	writers := []storage.ObjectWriter{baseWriter, localWriter}
	if err = s.write(ctx, writers, first.Content); err != nil {
		return
	}
	var req *server.UploadObjectRequest
	for {
		if req, err = srv.Recv(); err != nil {
			return
		}
		if req == nil {
			break
		}
		if err = s.write(ctx, writers, first.Content); err != nil {
			return
		}
	}

	s.status.AddObject(ctx, req.Bucket, req.Object)
	srv.SendAndClose(&server.UploadObjectResponse{})
	return
}

func (s *Server) write(ctx context.Context, ws []storage.ObjectWriter, content []byte) (err error) {
	// TODO: parallel write
	for _, w := range ws {
		if err = w.Write(ctx, content); err != nil {
			return
		}
	}
	return
}

func (s *Server) Heartbeat(ctx context.Context, req *server.HeartbeatRequest) (resp *server.HeartbeatResponse, err error) {
	// TODO: check s.id == req.serverID
	var ev server.ObjectEvent
	ev, err = s.status.HeartbeatStatus(ctx, req.LastBeatSeq)
	resp.BeatSeq = req.BeatSeq
	resp.Status = &server.Status{ObjectEvent: &ev}
	return
}
