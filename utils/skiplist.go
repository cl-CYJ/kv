package utils

import "sync/atomic"

const (
	maxHeight = 20
)

type node struct {
	keyOffset uint32
	keySize   uint32
	value     uint64
	height    uint64
	tower     [maxHeight]uint32
}

type Skiplist struct {
	height     int32
	headOffset uint32
	ref        int32
	arena      *Arena
	Close      func()
}

func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.Close != nil {
		s.Close()
	}
	s.arena = nil
}

func NewSkiplist(Size int64) *Skiplist {
	arena := newArena(Size)
	head :=
}
