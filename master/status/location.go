package status

import (
	"context"
	"sync"

	"github.com/engula/shared-storage/master/orchestrator"
)

type ServerId uint32

type ServerInfo struct {
	Id             uint32
	Addr           string
	LastAckBeatSeq uint64
	Status         ServerStatus
}

type ServerStatus int

const (
	Unknown ServerStatus = iota
	Normal
	Unreachable
)

type BucketObject struct {
	bucket string
	object string
}

type Locations struct {
	sync.Mutex
	srv       map[ServerId]*ServerInfo               // ServerID -> Addr
	objectSrv map[BucketObject]map[ServerId]struct{} // Object -> ServerId list
	bucketSrv map[string]map[ServerId]struct{}       // Bucket -> ServerId list
}

func NewLocations() (loc *Locations, err error) {
	loc = &Locations{
		srv:       make(map[ServerId]*ServerInfo),
		objectSrv: make(map[BucketObject]map[ServerId]struct{}),
	}
	return
}

func (l *Locations) Recovery(ctx context.Context) (err error) {
	return
}

func (l *Locations) UpdateSrv(nodes []orchestrator.Node) {
	if len(l.srv) != 0 {
		// TODO: handle add or remove node during running
		return
	}
	s := make(map[ServerId]*ServerInfo)
	for _, node := range nodes {
		s[ServerId(node.Id)] = &ServerInfo{Addr: node.Addr, Status: Unknown, Id: node.Id}
	}
	l.Lock()
	l.srv = s
	l.Unlock()
}

func (l *Locations) CurrentNodes() (nodes []*ServerInfo) {
	l.Lock()
	for _, s := range l.srv {
		nodes = append(nodes, s)
	}
	l.Unlock()
	return
}
