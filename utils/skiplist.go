package utils

import (
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"sync/atomic"
)

const (
	maxHeight = 20
	levelPro  = float64(0.25)
)

type node struct {
	keyOffset uint32
	keySize   uint16
	value     uint64
	height    uint16
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

func encodeValue(valOffset, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func (n *node) getValueOffset() (uint32, uint32) {
	v := atomic.LoadUint64(&n.value)
	return decodeValue(v)
}

func decodeValue(value uint64) (valOffset, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func (n *node) key(a *Arena) []byte {
	return a.getKey(n.keyOffset, n.keySize)
}

// 原子更新
func (n *node) setValue(a *Arena, v uint64) {
	atomic.StoreUint64(&n.value, v)
}

// 原子更新节点的next指针
func (n *node) casNextOffset(h int, bef, af uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], bef, af)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func newNode(a *Arena, key []byte, value ValueStrcut, height int) *node {
	nodeOffset := a.putNode(height)
	keyOffset := a.putKey(key)
	v := encodeValue(a.putValue(value), value.EncodeSize())
	node := a.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = v
	return node
}

func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, ValueStrcut{}, maxHeight)
	offset := arena.getNodeOffset(head)
	return &Skiplist{
		height:     1,
		headOffset: offset,
		ref:        1,
		arena:      arena,
	}
}

func (s *Skiplist) getNext(nd *node, h int) *node {
	return s.arena.getNode(nd.getNextOffset(h))
}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Float64() < levelPro {
		h++
	}
	return h
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		next := s.getNext(x, level)
		if next == nil {
			if level > 0 {
				level--
				continue
			}
			if !less {
				return nil, false
			}
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp > 0 {
			x = next
			continue
		}
		if cmp == 0 {
			if allowEqual {
				return next, true
			}
			if !less {
				return s.getNext(next, 0), false
			}
			if level > 0 {
				level--
				continue
			}
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}
		if level > 0 {
			level--
			continue
		}
		if !less {
			return next, false
		}
		if x == s.getHead() {
			return nil, false
		}
		return x, false
	}
}

// 在跳表的某一高度上寻找合适的插入位置，相当于单链表的查询
func (s *Skiplist) findPosInLevel(key []byte, pre uint32, h int) (uint32, uint32) {
	for {
		preNode := s.arena.getNode(pre)
		next := preNode.getNextOffset(h)
		nextNode := s.arena.getNode(next)
		if nextNode == nil {
			return pre, next
		}
		nextKey := nextNode.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			return next, next
		}
		if cmp < 0 {
			return pre, next
		}
		pre = next
	}
}

func (s *Skiplist) Search(key []byte) ValueStrcut {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return ValueStrcut{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return ValueStrcut{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getValue(valOffset, valSize)
	return vs
}

func (s *Skiplist) Add(key []byte, v ValueStrcut) {
	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	prev[listHeight] = s.headOffset
	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findPosInLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			valueOffset := s.arena.putValue(v)
			curNode := s.arena.getNode(prev[i])
			value := encodeValue(valueOffset, v.EncodeSize())
			curNode.setValue(s.arena, value)
			return
		}
	}

	height := s.randomHeight()
	nd := newNode(s.arena, key, v, height)
	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			break
		}
		listHeight = s.getHeight()
	}

	for i := 0; i < height; i++ {
		for {
			if s.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 1)
				prev[i], next[i] = s.findPosInLevel(key, s.headOffset, i)
				AssertTrue(prev[i] != next[i])
			}
			nd.tower[i] = next[i]
			preNode := s.arena.getNode(prev[i])
			if preNode.casNextOffset(i, next[i], s.arena.getNodeOffset(nd)) {
				break
			}
			prev[i], next[i] = s.findPosInLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				vo := s.arena.putValue(v)
				encValue := encodeValue(vo, v.EncodeSize())
				prevNode := s.arena.getNode(prev[i])
				prevNode.setValue(s.arena, encValue)
				return
			}
		}
	}
}

func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
