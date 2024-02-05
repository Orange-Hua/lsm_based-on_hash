package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kv/crc"
	"kv/internal"
	"kv/iterator"
	"sort"

	"github.com/golang/snappy"
)

type block []byte

func encodeBlockHandle(dst []byte, b blockHandle) int {
	n := binary.PutUvarint(dst, b.offset)
	m := binary.PutUvarint(dst[n:], b.length)
	return n + m
}

func decodeBlockHandle(v []byte) (blockHandle, int) {
	offset, n0 := binary.Uvarint(v)
	length, n1 := binary.Uvarint(v[n0:])
	if n0 == 0 || n1 == 0 {
		return blockHandle{}, 0
	}
	return blockHandle{offset: offset, length: length}, n0 + n1
}

type scmp interface {
	Compare([]byte, []byte) int
}

/*
冷知识记忆：重启点记录数据kv的偏移
数据块和元数据和元数据索引块结构是相同的，kv数据+块类型+校验和
*/
func (b block) seek(c internal.SeparatorByteComparer, key []byte) (*blockIter, error) {

	//todo获取重启点数目，
	numRestart := int(binary.LittleEndian.Uint32(b[len(b)-4:]))
	if numRestart == 0 {
		return nil, errors.New("block没有重启点")
	}
	//第一个重启点的偏移
	n := len(b) - (numRestart+1)*4
	var offset = 0
	if len(key) > 0 {
		//找重启点中第一个大于给定key的偏移，对应的索引
		index := sort.Search(numRestart, func(i int) bool {
			o := int(binary.LittleEndian.Uint32(b[n+i*4:]))
			o++ //偏移对应的kv段，共享key长度为0,占一个字节
			unshareKeyLen, n1 := binary.Uvarint(b[o:])
			_, n2 := binary.Uvarint(b[o+n1:])
			unshareKey := b[o+n1+n2 : o+n1+n2+int(unshareKeyLen)]
			return c.CompareUniqeKey(unshareKey, key) > 0
		})

		if index > 0 {
			offset = int(binary.LittleEndian.Uint32(b[n+(index-1)*4:]))
		}
	}

	biter := &blockIter{
		data: b[offset:n],
		key:  make([]byte, 0, 256),
	}
	for biter.Next() && c.CompareUniqeKey(biter.Key(), key) < 0 {
		//将迭代器调整到第一个比给定key大的key
	}

	if biter.err != nil {
		return nil, biter.err
	}
	biter.soi = !biter.eoi //soi为true，eoi为false
	return biter, nil

}

type blockIter struct {
	data     []byte //初始化处，block.seek()，范围：小于key的最大重启点offset,到该数据块末尾
	key, val []byte

	err      error
	soi, eoi bool
	index    int
}

/*
遍历每个kv
*/
func (i *blockIter) Next() bool {
	if i.eoi || i.err != nil {
		return false
	}
	if i.soi {
		i.soi = false
		return true
	}
	if len(i.data) == 0 {
		i.Close()
		return false
	}
	//sharekeyLen, n0 := binary.Uvarint(i.data[:])
	sharekeyLen, n0 := binary.Uvarint(i.data[:])
	unshareKeyLen, n1 := binary.Uvarint(i.data[n0:])
	valueLen, n2 := binary.Uvarint(i.data[n0+n1:])
	m := n0 + n1 + n2
	//i.key[:0]出现bug

	i.key = append(i.key[:sharekeyLen], i.data[m:m+int(unshareKeyLen)]...)
	//i.r = i.r + int(unshareKeyLen)
	//i.key = i.data[m : m+int(unshareKeyLen)]
	i.val = i.data[m+int(unshareKeyLen) : m+int(unshareKeyLen)+int(valueLen)]
	i.data = i.data[m+int(unshareKeyLen+valueLen):]
	return true
}

func (i *blockIter) Key() []byte {
	if i.soi {
		return nil
	}
	return i.key[:len(i.key):len(i.key)]
}

func (i *blockIter) Value() []byte {
	if i.soi {
		return nil
	}
	return i.val[:len(i.val):len(i.val)]
}

func (i *blockIter) Close() error {
	i.eoi = true
	i.key = nil
	i.val = nil

	return i.err
}
func (i *blockIter) First() bool {
	return i.soi == true && i.eoi == false
}

type tableIter struct {
	reader *Reader
	data   *blockIter //
	index  *blockIter //数据索引块迭代器
	err    error
}

func (i *tableIter) Pair() *iterator.Pair {
	return &iterator.Pair{Key: i.data.Key(), Value: i.data.Value()}
}
func (i *tableIter) First() bool {
	return true
}
func (i *tableIter) nextBlock(key []byte, fr *filterReader) bool {
	if !i.index.Next() {
		i.err = i.index.err
		return false
	}
	v := i.index.Value()
	h, n := decodeBlockHandle(v)
	if n == 0 || n != len(v) {
		i.err = errors.New("corrupt index entry")
		return false
	}
	if fr != nil && !fr.mayContain(h.offset, key) {
		i.err = errors.New("filter not found key")
		return false
	}
	k, err := i.reader.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}
	data, err := k.seek(i.reader.o.dataCmp, key)
	i.data = data
	return true
}

func (i *tableIter) Next() bool {
	if i.data == nil {
		return false
	}
	for {
		if i.data.Next() {
			return true
		}
		if i.data.err != nil {
			i.err = i.data.err
			break
		}
		if !i.nextBlock(nil, nil) {
			break
		}
	}
	i.Close()
	return false
}
func (i *tableIter) Key() []byte {
	return i.data.Key()

}

func (i *tableIter) Value() []byte {
	return i.data.Value()
}
func (i *tableIter) Close() error {
	i.data = nil
	return i.err
}

type filterReader struct {
	data    []byte
	offsets []byte // len(offsets) must be a multiple of 4.
	policy  internal.FilterPolicy
	shift   uint32
}

func (f *filterReader) valid() bool {
	return f.data != nil
}

func (f *filterReader) init(data []byte, policy internal.FilterPolicy) (ok bool) {
	if len(data) < 5 {
		return false
	}
	lastOffset := binary.LittleEndian.Uint32(data[len(data)-5:])
	if uint64(lastOffset) > uint64(len(data)-5) {
		return false
	}
	data, offsets, shift := data[:lastOffset], data[lastOffset:len(data)-1], uint32(data[len(data)-1])
	if len(offsets)&3 != 0 {
		return false
	}
	f.data = data
	f.offsets = offsets
	f.policy = policy
	f.shift = shift
	return true
}

func (f *filterReader) mayContain(blockOffset uint64, key []byte) bool {
	index := blockOffset >> f.shift
	if index >= uint64(len(f.offsets)/4-1) {
		return true
	}
	i := binary.LittleEndian.Uint32(f.offsets[4*index+0:])
	j := binary.LittleEndian.Uint32(f.offsets[4*index+4:])
	if i >= j || uint64(j) > uint64(len(f.data)) {
		return true
	}
	return f.policy.MayContain(f.data[i:j], key)
}

type Reader struct {
	file   internal.File
	err    error
	index  block
	o      *ReadOpt
	filter filterReader
	cmp    internal.SeparatorByteComparer
}

func (r *Reader) readBlock(bh blockHandle) (block, error) {
	b := make([]byte, bh.length+blockTrailerLen)
	if _, err := r.file.ReadAt(b, int64(bh.offset)); err != nil {
		return nil, err
	}
	if r.o != nil && r.o.verifyChecksums {
		desireCheckSum := binary.LittleEndian.Uint32(b[bh.length+1:])
		actualCheckSum := crc.New(b[:bh.length+1]).Value()
		if desireCheckSum != actualCheckSum {
			return nil, errors.New("校验和不相等")
		}
	}

	switch b[bh.length] {
	case noCompressionBlockType:
		return b[:bh.length], nil
	case snappyCompressionBlockType:
		b, err := snappy.Decode(nil, b[:bh.length])
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	return nil, fmt.Errorf("leveldb/table: unknown block compression: %d", b[bh.length])

}
func (r *Reader) Find(key []byte) iterator.Iterator {
	return r.find(key, nil)
}

// 数据index块，记录每一块的最小key对应的handle，找到对应的数据块大于等于给定key的位置
func (r *Reader) find(key []byte, f *filterReader) iterator.Iterator {
	if r.err != nil {
		return &tableIter{err: r.err}
	}

	index, err := r.index.seek(r.o.kcmp, key)
	if err != nil {
		return &tableIter{err: err}
	}
	i := &tableIter{
		reader: r,
		index:  index,
	}
	i.nextBlock(key, f)
	return i
}
func (r *Reader) Close() error {
	r.o = nil
	if err := r.file.Close(); err != nil {
		r.err = err
	}
	return r.err
}
func (r *Reader) readMetaindex(metaindexBH blockHandle) error {
	if r.o == nil {
		return nil
	}
	fp := r.o.fp
	if fp == nil {
		return nil
	}
	b, err := r.readBlock(metaindexBH)
	if err != nil {
		return err
	}
	i, err := b.seek(r.cmp, nil)
	if err != nil {
		return err
	}
	fileName := "filter." + fp.Name()
	filterBH := blockHandle{}
	for i.Next() {
		if string(i.Key()) != fileName {
			continue
		}
		var n int
		filterBH, n = decodeBlockHandle(i.Value())
		if n == 0 {
			return errors.New("解析filter-block-handle出错")
		}
		break
	}
	if err := i.Close(); err != nil {
		return err
	}
	if filterBH != (blockHandle{}) {
		b, err := r.readBlock(filterBH)
		if err != nil {
			return err
		}
		if !r.filter.init(b, fp) {
			return errors.New("leveldb/table: invalid table (bad filter block)")
		}
	}
	return nil
}

func NewReader(f internal.File, o *ReadOpt) *Reader {

	r := &Reader{
		file: f,
		o:    o,
	}
	r.cmp = r.o.dataCmp
	if f == nil {
		r.err = errors.New("新建reader时，file是nil")
		return r
	}

	stat, err := f.Stat()
	if err != nil {
		r.err = fmt.Errorf("leveldb/table: invalid table (could not stat file): %v", err)
		return r
	}
	var footer [footerLen]byte
	if stat.Size() < int64(len(footer)) {
		r.err = errors.New("leveldb/table: invalid table (file size is too small)")
		return r
	}

	_, err = f.ReadAt(footer[:], stat.Size()-int64(len(footer)))
	if err != nil && err != io.EOF {
		r.err = fmt.Errorf("leveldb/table: invalid table (could not read footer): %v", err)
		return r
	}

	if string(footer[footerLen-len(magic):footerLen]) != magic {
		r.err = errors.New("leveldb/table: invalid table (bad magic number)")
		return r
	}

	//读元数据索引handle
	metaIndexHandle, n := decodeBlockHandle(footer[:])
	if n == 0 {
		r.err = errors.New("leveldb/table: invalid table (bad metaindex block handle)")
		return r
	}
	err = r.readMetaindex(metaIndexHandle)
	if err != nil {
		r.err = err
		return r
	}

	//读数据索引
	dataIndexHandle, n := decodeBlockHandle(footer[n:])
	if n == 0 {
		r.err = errors.New("leveldb/table: invalid table (bad index block handle)")
		return r
	}
	r.index, r.err = r.readBlock(dataIndexHandle) //文件的数据key索引
	return r

}
