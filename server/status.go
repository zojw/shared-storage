package server

import (
	"context"
	"sync"

	proto "github.com/engula/shared-storage/proto/server"
	"github.com/engula/shared-storage/server/storage"
)

type Status struct {
	sync.Mutex

	last_beat_seq uint64

	buckets map[string]struct{}
	objects map[BucketObject]struct{}
	delta   []interface{}

	local storage.Storage
}

type BucketObject struct {
	Bucket string
	Object string
}

type EventType int

const (
	AddBucket EventType = iota
	DeleteBucket
	AddObject
	DeleteObject
)

type Event struct {
	Typ    EventType
	Bucket string
	Object string
}

func NewStatus(ctx context.Context, local storage.Storage) (s *Status, err error) {
	s = &Status{
		local: local,
	}
	s.buckets = make(map[string]struct{})
	s.objects = make(map[BucketObject]struct{})
	if err = s.Recovery(ctx); err != nil {
		return
	}
	return
}

func (s *Status) Recovery(ctx context.Context) (err error) {
	buckets, objects := make(map[string]struct{}), make(map[BucketObject]struct{})
	var (
		bs []string
		os []string
	)
	if bs, err = s.local.ListBuckets(ctx); err != nil {
		return
	}
	for _, b := range bs {
		buckets[b] = struct{}{}
		if os, err = s.local.ListObjects(ctx, b); err != nil {
			return
		}
		for _, o := range os {
			objects[BucketObject{Bucket: b, Object: o}] = struct{}{}
		}
	}

	s.Lock()
	s.buckets = buckets
	s.objects = objects
	s.Unlock()
	return
}

func (s *Status) AddBucket(ctx context.Context, bucket string) {
	s.Lock()
	s.buckets[bucket] = struct{}{}
	s.delta = append(s.delta, Event{Typ: AddBucket, Bucket: bucket})
	s.Unlock()
}

func (s *Status) DeleteBucket(ctx context.Context, bucket string) {
	s.Lock()
	delete(s.buckets, bucket)
	s.delta = append(s.delta, Event{Typ: DeleteBucket, Bucket: bucket})
	s.Unlock()
}

func (s *Status) AddObject(ctx context.Context, bucket, object string) {
	s.Lock()
	s.objects[BucketObject{Bucket: bucket, Object: object}] = struct{}{}
	s.delta = append(s.delta, Event{Typ: AddObject, Bucket: bucket, Object: object})
	s.Unlock()
}

func (s *Status) DeleteObject(ctx context.Context, bucket, object string) {
	s.Lock()
	delete(s.objects, BucketObject{Bucket: bucket, Object: object})
	s.delta = append(s.delta, Event{Typ: DeleteObject, Bucket: bucket, Object: object})
	s.Unlock()
}

func (s *Status) HeartbeatStatus(ctx context.Context, lastBeatSeq uint64) (ev proto.ObjectEvent, err error) {
	// TODO: generate delta instead of always full
	s.Lock()
	buckets := make([]string, 0, len(s.buckets))
	for b := range s.buckets {
		buckets = append(buckets, b)
	}
	objects := make([]*proto.BucketObject, 0, len(s.objects))
	for o := range s.objects {
		objects = append(objects, &proto.BucketObject{Bucket: o.Bucket, Object: o.Object})
	}
	s.Unlock()
	ev = proto.ObjectEvent{
		Typ:         proto.ObjectEvent_Full,
		BucketAdded: buckets,
		ObjectAdded: objects,
	}
	return
}
