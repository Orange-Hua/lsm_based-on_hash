package storage

import (
	"container/list"
	"kv/storage/record"
	"sync"
	"sync/atomic"
)

type versionEditManager struct {
	writeWaitList *list.List

	vs                  *VersionSet
	l0CompactionTrigger int
	l0MaxWritesTrigger  int
	manifestMutex       sync.Mutex
	writeMutex          sync.Mutex
	fm                  StorageManager
	manifest            *record.Writer
	waitNum             atomic.Int32
	syn                 chan struct{}
	maxBufferNum        int
}

func newVersionEditManager(vs *VersionSet, zeroLevelCompactionLength, maxBufferNum int, fm StorageManager) *versionEditManager {
	vem := &versionEditManager{
		writeWaitList: list.New(),
		vs:            vs,
		//zeroLevelCompactionLength: zeroLevelCompactionLength,
		maxBufferNum: maxBufferNum,
		fm:           fm,
	}
	vem.syn = make(chan struct{}, 1)
	return vem
}
