package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkiplist(1000)

	//Put & Get
	value1 := NewValueStruct([]byte("value1"))
	key1 := []byte("key1-20230104")
	list.Add(key1, *value1)
	vs := list.Search(key1)
	assert.Equal(t, value1.Value, vs.Value)

	key2 := []byte("key2-20230104")
	value2 := NewValueStruct([]byte("value2"))
	list.Add(key2, *value2)
	vs = list.Search(key2)
	assert.Equal(t, value2.Value, vs.Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte("key3-20230104")).Value)

	//Update a entry
	newValue2 := NewValueStruct([]byte("newValue2"))
	list.Add(key2, *newValue2)
	assert.Equal(t, newValue2.Value, list.Search(key2).Value)
}
