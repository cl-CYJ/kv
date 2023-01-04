package utils

import (
	"encoding/binary"
)

type ValueStrcut struct {
	Value     []byte
	ExpiredAt uint64
}

func (v *ValueStrcut) EncodeSize() uint32 {
	valueSize := len(v.Value)
	encSize := sizeVarint(v.ExpiredAt)
	return uint32(valueSize + encSize)
}

func NewValueStruct(value []byte) *ValueStrcut {
	return &ValueStrcut{
		Value: value,
	}
}

func sizeVarint(v uint64) (size int) {
	for {
		size++
		v >>= 7
		if v == 0 {
			break
		}
	}
	return size
}
func (v *ValueStrcut) EncodeValue(b []byte) uint32 {
	sz := binary.PutUvarint(b[:], v.ExpiredAt)
	num := copy(b[sz:], v.Value)
	return uint32(sz + num)
}

func (v *ValueStrcut) DecodeValue(buf []byte) {
	var size int
	v.ExpiredAt, size = binary.Uvarint(buf)
	v.Value = buf[size:]
}
