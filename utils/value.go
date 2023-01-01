package utils

type ValueStrcut struct {
	Value     []byte
	ExpiredAt uint64
}

func (v *ValueStrcut) EncodeSize() uint32 {
	valueSize := len(v.Value)
	encSize := sizeVarint(v.ExpiredAt)
	return uint32(valueSize + encSize)
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
