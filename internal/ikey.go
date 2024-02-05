package internal

import (
	"encoding/binary"

	"unsafe"
)

type Item struct {
	key   InternalKey
	value any
	zone  uint8
	succ  chan error
}

type InternalKey struct {
	UniqeKey
	SeqNum   uint64
	Kind     InternalKeyKind
	ValueLen uint64
}

type UniqeKey struct {
	Key         uint64
	ConflictKey uint64
}

func Kind(key []byte) InternalKeyKind {
	if key[24] == byte(InternalKeyKindDelete) {
		return InternalKeyKindDelete
	}
	if key[24] == byte(InternalKeyKindSet) {
		return InternalKeyKindSet
	}
	return 0
}
func NewInternalKey(key, conflictKey uint64, seq uint64) *InternalKey {
	return &InternalKey{
		UniqeKey: UniqeKey{
			Key:         key,
			ConflictKey: conflictKey,
		},
		SeqNum: seq,
	}
}
func ByteArrayToInternalKey(a []byte) *InternalKey {

	key := binary.BigEndian.Uint64(a)
	ckey := binary.BigEndian.Uint64(a[8:])
	seq := binary.BigEndian.Uint64(a[16:])
	kind := a[24]
	valueLen := binary.BigEndian.Uint64(a[25:])

	return &InternalKey{UniqeKey{Key: key, ConflictKey: ckey}, seq, (InternalKeyKind)(kind), valueLen}
}

func UniqueKeyToByteArray(a *UniqeKey) []byte {
	var res []byte
	res = binary.BigEndian.AppendUint64(res, a.Key)
	res = binary.BigEndian.AppendUint64(res, a.ConflictKey)
	return res
}
func InternalKeyToByteArray(a *InternalKey) []byte {
	var res []byte
	res = binary.BigEndian.AppendUint64(res, a.Key)
	res = binary.BigEndian.AppendUint64(res, a.ConflictKey)
	res = binary.BigEndian.AppendUint64(res, a.SeqNum)
	res = append(res, byte(a.Kind))
	res = binary.BigEndian.AppendUint64(res, a.ValueLen)
	return res
	// temp := (*byte)(unsafe.Pointer(&a))
	// start := unsafe.Pointer(temp)

	// res := make([]byte, size)
	// for i := 0; i < int(size); i++ {
	// 	s := unsafe.Pointer(uintptr(start) + uintptr(i))
	// 	res[i] = *(*byte)(s)
	// }

	// fmt.Println(res)
	// b := ByteArrayToInternalKey(res)
	// k := b.key
	// c := b.conflictKey
	// s := b.seqNum
	// _ = k
	// _ = c
	// _ = s
	// return nil
}

type InternalKeyKind uint8

/*
8字节key
8字节conkey
8字节序列号
1字节zone
1字节kind
varint value长度
*/
const (
	// These constants are part of the file format, and should not be changed.
	InternalKeyKindDelete   InternalKeyKind = 1
	InternalKeyKindSet      InternalKeyKind = 0
	InternalKeyKindUpBuffer InternalKeyKind = 2
	// This maximum value isn't part of the file format. It's unlikely,
	// but future extensions may increase this value.
	//
	// When constructing an internal key to pass to DB.Find, internalKeyComparer
	// sorts decreasing by kind (after sorting increasing by user key and
	// decreasing by sequence number). Thus, use internalKeyKindMax, which sorts
	// 'less than or equal to' any other valid internalKeyKind, when searching
	// for any kind of internal key formed by a certain user key and seqNum.
	InternalKeyKindMax InternalKeyKind = 3

	BUFFER = 1
	TABLE  = 2
	DISK   = 3
)

type UplevelInactive struct {
	WantNum int
	Ukeys   []UniqeKey
}
type Zone int8

// internalKeySeqNumMax is the largest valid sequence number.
const internalKeySeqNumMax = uint64(1<<56 - 1)

func makeInternalKey(key, value any, seq uint64, z int8) InternalKey {
	var store [32]byte
	binary.LittleEndian.PutUint64(store[:], 32)
	binary.LittleEndian.PutUint64(store[8:], 32)
	binary.LittleEndian.PutUint64(store[16:], seq)
	binary.LittleEndian.PutUint32(store[24:], uint32(unsafe.Sizeof(value)))
	store[28] = byte(z)

	return InternalKey{}
}

type Pair struct { //容器节点存储的是值
	Key   *InternalKey
	Value []byte
}

type Chunk struct {
	Index             int
	Data              []*Pair
	Smallest, Largest *InternalKey
	Cmp               Comparer
	Callback          func()
}

func (c *Chunk) Len() int {
	return len(c.Data)
}

func (c *Chunk) Less(i, j int) bool {
	var a int
	if c.Cmp == nil {
		c.Cmp = DefaultComparer
	}
	a = c.Cmp.CompareKey(c.Data[i].Key, c.Data[j].Key)

	return a < 0
}

func (c *Chunk) Swap(i, j int) {
	c.Data[i], c.Data[j] = c.Data[j], c.Data[i]
}
