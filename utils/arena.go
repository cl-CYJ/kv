package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	MaxNodeSize = int(unsafe.Sizeof(node{}))
	Align       = int(unsafe.Sizeof(uint64(0))) - 1
	// 分配一个32位无符号整数所占用的内存大小，Level中存的是内存地址，即uint32的大小
	OffsetSize = int(unsafe.Sizeof(uint32(0)))
)

type Arena struct {
	offset uint32
	buf    []byte
}

func newArena(size int64) *Arena {
	ret := &Arena{
		// offset从1起，0作为空指针标识
		offset: 1,
		buf:    make([]byte, size),
	}
	return ret
}

func (a *Arena) allocate(size uint32) uint32 {
	offset := atomic.AddUint32(&a.offset, size)
	// todo：扩容不是并发安全的，还是需要加锁
	if int(offset) > len(a.buf)-int(size) {
		newBuf := make([]byte, 2*len(a.buf))
		copy(newBuf, a.buf)
		a.buf = newBuf
	}
	return offset - size
}

func (a *Arena) size() int64 {
	return int64(atomic.LoadUint32(&a.offset))
}

func (a *Arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * OffsetSize
	enoughSize := uint32(MaxNodeSize - unusedSize + Align)
	n := a.allocate(enoughSize)
	m := (n + uint32(Align)) & ^uint32(Align)
	return m
}

func (a *Arena) putKey(key []byte) uint32 {
	keySize := uint32(len(key))
	offset := a.allocate(keySize)
	buf := a.buf[offset : offset+keySize]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (a *Arena) putValue(v ValueStrcut) uint32 {
	size := v.EncodeSize()
	offset := a.allocate(size)
	v.EncodeValue(a.buf[offset:])
	return offset
}

func (a *Arena) getKey(offset uint32, size uint16) []byte {
	return a.buf[offset : offset+uint32(size)]
}

func (a *Arena) getValue(offset uint32, size uint32) (v ValueStrcut) {
	v.DecodeValue(a.buf[offset : offset+size])
	return
}

func (a *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
