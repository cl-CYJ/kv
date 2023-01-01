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
		offset: 0,
		buf:    make([]byte, size),
	}
	return ret
}

func (a *Arena) allocate(size uint32) uint32 {
	offset := atomic.AddUint32(&a.offset, size)
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

func (a *Arena) putValue(v V)

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
