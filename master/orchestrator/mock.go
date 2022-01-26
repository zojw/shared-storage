package orchestrator

import "context"

var (
	_ Orchestrator = &Mock{}
)

type Mock struct {
	Nodes []string
}

func (m *Mock) ListCacheNode(ctx context.Context) (nodes []string, err error) {
	nodes = m.Nodes
	return
}
