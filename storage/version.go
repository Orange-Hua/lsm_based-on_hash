package storage

import (
	"fmt"
	"kv/internal"
	"kv/iterator"
	"log"
	"sync/atomic"
)

type fileMetadata struct {
	fileNum           uint64
	size              uint64
	smallest, largest []byte
	entry             uint
	ref               uint
}

type byFileNum []fileMetadata

func (b byFileNum) Len() int           { return len(b) }
func (b byFileNum) Less(i, j int) bool { return b[i].fileNum < b[j].fileNum }
func (b byFileNum) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type bySmallest struct {
	dat []fileMetadata
	cmp internal.SeparatorByteComparer
}

func (b bySmallest) Len() int { return len(b.dat) }
func (b bySmallest) Less(i, j int) bool {
	return b.cmp.CompareKey(b.dat[i].smallest, b.dat[j].smallest) < 0
}
func (b bySmallest) Swap(i, j int) { b.dat[i], b.dat[j] = b.dat[j], b.dat[i] }

func totalSize(fm []fileMetadata) (size uint64) {
	for _, f := range fm {
		size += f.size
	}
	return
}

func ikeyRange(icmp Comparer, f0, f1 []fileMetadata) (smallest, largest []byte) {
	first := true
	for _, levelFiles := range [2][]fileMetadata{f0, f1} {
		for _, fileMeta := range levelFiles {
			if first {
				first = false
				smallest = fileMeta.smallest
				largest = fileMeta.largest
			}
			if icmp.Compare(fileMeta.smallest, smallest) < 0 {
				smallest = fileMeta.smallest
			}
			if icmp.Compare(fileMeta.largest, largest) > 0 {
				largest = fileMeta.largest
			}
		}
	}
	return

}

const numLevels = 4

type Version struct {
	files           [numLevels][][]fileMetadata
	deletes         []uint64
	ref             atomic.Int32
	prev, next      *Version
	compactionScore float64
	compactionLevel int
}
type tableIkeyFinder interface {
	find(fileNum uint64, ikey []byte, ropt *ReadOpt) (iterator.Iterator, error)
}

/*
因为0层是重叠的，一个meta可能和原始的ukey0,ukey1没有重叠，但后续的meta和ukey0,ukey1有
重叠，且这个meta的small可能比ukey0小，larget可能比ukey1大，和这个meta重叠的，和ukey0,
ukey1不重叠的也要算在内，也算和ukey0，ukey1重叠
*/

func internalFind(tiFinder tableIkeyFinder, fileNum uint64, ikey []byte, ropt *ReadOpt) ([]byte, bool, error) {
	iter, err := tiFinder.find(fileNum, ikey, ropt)

	if err != nil {
		return nil, false, fmt.Errorf("leveldb: could not open table %d: %v", fileNum, err)
	}
	//conclusive,key找到了
	value, conclusive, err := internalGet(iter, ikey)
	if conclusive {
		return value, true, err
	}
	return nil, false, err
}
func (v *Version) get(partNum int, ikey []byte, tiFinder tableIkeyFinder, ropt *ReadOpt) ([]byte, bool, error) {

	if len(v.files) == 0 {
		log.Fatalf("写入数据出错\r\n")
	}
	for _, f := range v.files[0][0] {
		if ropt.dataCmp.CompareUniqeKey(ikey, f.smallest) < 0 {
			continue
		}
		if ropt.dataCmp.CompareUniqeKey(ikey, f.largest) > 0 {
			continue
		}

		value, ok, err := internalFind(tiFinder, f.fileNum, ikey, ropt)
		if ok {
			return value, true, err
		}
	}
	var err error
	for i := 1; i < len(v.files); i++ {
		for _, f := range v.files[i][partNum] {
			if value, ok, err := internalFind(tiFinder, f.fileNum, ikey, nil); ok {
				return value, ok, err
			}
		}

	}
	return nil, false, err

}
func internalGet(iter iterator.Iterator, key []byte) (value []byte, conclusive bool, err error) {
	if !iter.Next() {
		iter.Close()
		return nil, true, nil
	}
	k := iter.Key()
	ik := internal.ByteArrayToInternalKey(k)
	ikey := internal.ByteArrayToInternalKey(key)
	if internal.DefaultComparer.CompareUniqeKey(&ik.UniqeKey, &ikey.UniqeKey) != 0 {
		iter.Close()
		return nil, false, nil
	}
	if ik.Kind == internal.InternalKeyKindDelete {
		iter.Close()
		return nil, true, nil
	}
	return iter.Value(), true, nil

}

func (v *Version) addRef() int {
	old := v.ref.Load()
	res := 0
	for {
		if v.ref.CompareAndSwap(old, old+1) {
			res = int(old + 1)
			break
		}

		old = v.ref.Load()
	}
	return res

}

func (v *Version) delRef() {
	old := v.ref.Load()
	if old <= 0 {
		log.Println("version减引用为负数")
		return
	}
	for {
		if v.ref.CompareAndSwap(old, old-1) {
			break
		}
		old = v.ref.Load()
		if old == 0 {
			break
		}
	}

}
