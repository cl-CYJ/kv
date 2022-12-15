package utils

import "sync/atomic"

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
