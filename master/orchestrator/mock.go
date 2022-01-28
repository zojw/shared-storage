package orchestrator

import "context"

var (
	_ Orchestrator = &Mock{}
)

type Mock struct {
	Nodes []Node
}

func (m *Mock) ListCacheNode(ctx context.Context) (nodes []Node, err error) {
	nodes = m.Nodes
	return
}
