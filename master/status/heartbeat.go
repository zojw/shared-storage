package status

import (
	"context"
	"log"
	"time"

	orch "github.com/engula/shared-storage/master/orchestrator"
	serverpb "github.com/engula/shared-storage/proto/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	heartbeatInterval = 500 * time.Millisecond
)

type HeartbeatCycle struct {
	orch orch.Orchestrator
	locs *Locations

	stopCh chan struct{}
}

func NewHeartbeatCycle(o orch.Orchestrator, l *Locations) (h *HeartbeatCycle) {
	h = &HeartbeatCycle{orch: o, locs: l, stopCh: make(chan struct{})}
	return
}

func (h *HeartbeatCycle) Run() (err error) {
	t := time.NewTimer(heartbeatInterval)
	for {
		select {
		case <-h.stopCh:
			return
		case <-t.C:
			ctx := context.Background()
			if err = h.updateServer(ctx); err != nil {
				log.Print("update srv from orachester fail", err)
				err = nil
				t.Reset(heartbeatInterval)
				continue
			}
			h.sendHeartbeat(ctx)
			t.Reset(heartbeatInterval)
			return
		}
	}
}

func (h *HeartbeatCycle) Stop() {
	close(h.stopCh)
}

func (h *HeartbeatCycle) updateServer(ctx context.Context) (err error) {
	var nodes []orch.Node
	if nodes, err = h.orch.ListCacheNode(ctx); err != nil {
		return
	}
	h.locs.UpdateSrv(nodes)
	return
}

func (h *HeartbeatCycle) sendHeartbeat(ctx context.Context) {
	nodes := h.locs.CurrentNodes()
	for _, node := range nodes {
		h.sendOneNode(ctx, node.Addr)
	}
}

func (h *HeartbeatCycle) sendOneNode(ctx context.Context, addr string) {
	var (
		conn *grpc.ClientConn
		err  error
	)
	if conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		return
	}
	client := serverpb.NewCacheNodeServiceClient(conn)
	client.Heartbeat(ctx, &serverpb.HeartbeatRequest{})
}
