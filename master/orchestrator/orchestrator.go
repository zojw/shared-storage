package orchestrator

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	Id   uint32
	Addr string
}

type Orchestrator interface {
	ListCacheNode(ctx context.Context) (nodes []Node, err error)
}

func RunWithAllCacheNodes(ctx context.Context, o Orchestrator, f func(ctx context.Context, conn *grpc.ClientConn) error) (err error) {
	var nodes []Node
	if nodes, err = o.ListCacheNode(ctx); err != nil {
		return
	}
	for _, node := range nodes {
		if err = func() (err error) {
			// TODO: make conn pooled and parallel invoke
			var conn *grpc.ClientConn
			if conn, err = grpc.Dial(node.Addr, grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
				return
			}
			defer conn.Close()

			if err = f(ctx, conn); err != nil {
				return
			}
			return
		}(); err != nil {
			return
		}
	}
	return
}
