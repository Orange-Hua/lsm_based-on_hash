package storage

import (
	"kv/internal"
	"sync"
)

type diskLru struct {
	readBuffer chan internal.UniqeKey
	s          internal.SegmentLru
	rwmtx      sync.RWMutex
	maxSize    int
}

func newDiskLru(r chan internal.UniqeKey, maxSize int) *diskLru {

	d := diskLru{}
	d.readBuffer = r
	d.maxSize = maxSize
	d.s = internal.NewSegmentLru(internal.LRUACTIVENUM)
	return &d
}

func (t *diskLru) Put(key internal.UniqeKey) {

	t.s.Put(key)
	if t.s.Size() > t.maxSize {
		t.Victim()
	}
}

func (t *diskLru) MultiPut(keys []internal.UniqeKey) {

	for i := range keys {
		t.Put(keys[i])
	}

}
func (t *diskLru) Sync() {
	t.rwmtx.Lock()
	n := len(t.readBuffer)
	for i := 0; i < n; i++ {
		t.s.Put(<-t.readBuffer)
	}
	t.rwmtx.Unlock()
}

func (t *diskLru) GetFewActive(limit int, sync bool) []internal.UniqeKey {
	if sync {
		t.Sync()
	}
	t.rwmtx.RLock()
	res := t.s.GetFewActive(limit)
	t.rwmtx.RUnlock()
	return res
}

func (t *diskLru) GetVictim(sync bool) internal.UniqeKey {
	if sync {
		t.Sync()
	}
	t.rwmtx.RLock()
	res := t.s.GetVictim()
	t.rwmtx.RUnlock()
	return res
}

func (t *diskLru) Victim() internal.UniqeKey {

	return t.s.Victim()
}

func (t *diskLru) GetFewInactive(num int, sync bool) []internal.UniqeKey {
	if sync {
		t.Sync()
	}
	t.rwmtx.RLock()
	res := t.s.GetFewInactive(num)
	t.rwmtx.RUnlock()
	return res
}

func (t *diskLru) Del(key internal.UniqeKey) {
	t.rwmtx.Lock()
	t.s.RemoveNodeByKey(key)
	t.rwmtx.Unlock()
}
