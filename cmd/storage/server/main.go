package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	proto "github.com/engula/shared-storage/proto/server"
	"github.com/engula/shared-storage/server"
	"github.com/engula/shared-storage/server/storage"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	s3, err := storage.NewS3(ctx, "tenant1", "bucket1", "us-west-1")
	if err != nil {
		log.Fatal(err)
	}
	localFile, err := storage.NewLocalFile(ctx, filepath.Join(os.TempDir(), fmt.Sprintf("engula-test-%d", time.Now().UnixNano())))
	if err != nil {
		log.Fatal(err)
	}
	srv := server.NewServer(s3, localFile, nil)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 8081))
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	proto.RegisterBucketServiceServer(s, srv)
	proto.RegisterObjectServiceServer(s, srv)
	log.Printf("server listening at %v", lis.Addr())
	err = s.Serve(lis)
	if err != nil {
		log.Fatal(err)
	}
}
