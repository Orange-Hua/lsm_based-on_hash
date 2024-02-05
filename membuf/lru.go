package membuf

import (
	"kv/internal"
	"sync"
)

type bufLru struct {
	readBuffer chan internal.UniqeKey
	s          internal.Lru
	rwmtx      sync.RWMutex
}

func newBufLru(c chan internal.UniqeKey) *bufLru {
	b := bufLru{}
	b.s = internal.NewSegmentLru(internal.LRUACTIVENUM)
	b.readBuffer = c
	return &b
}

func (t *bufLru) Put(key internal.UniqeKey) {
	//t.rwmtx.Lock()
	t.s.Put(key)
	//t.rwmtx.Unlock()
}

func (t *bufLru) MultiPut(keys []internal.UniqeKey) {
	//t.rwmtx.Lock()
	for i := range keys {
		t.Put(keys[i])
	}
	//t.rwmtx.Unlock()
}

func (t *bufLru) GetFewActive(limit int, sync bool) []internal.UniqeKey {
	if sync {
		t.Sync()
	}
	t.rwmtx.RLock()
	ukeys := t.s.GetFewActive(limit)
	t.rwmtx.RUnlock()
	return ukeys
}

func (b *bufLru) Sync() {

	b.rwmtx.Lock()
	n := len(b.readBuffer)
	for i := 0; i < n; i++ {
		b.s.Put(<-b.readBuffer)
	}
	b.rwmtx.Unlock()

}
func (t *bufLru) GetVictim(sync bool) internal.UniqeKey {
	if sync {
		t.Sync()
	}
	var res internal.UniqeKey
	t.rwmtx.RLock()
	res = t.s.GetVictim()
	t.rwmtx.RUnlock()
	return res
}

func (t *bufLru) Victim(sync bool) internal.UniqeKey {
	if sync {
		t.Sync()
	}
	var res internal.UniqeKey
	t.rwmtx.Lock()

	res = t.s.Victim()
	t.rwmtx.Unlock()
	return res
}

func (t *bufLru) GetFewInactive(num int, sync bool) []internal.UniqeKey {
	if sync {
		t.Sync()
	}
	var res []internal.UniqeKey
	t.rwmtx.RLock()
	res = t.s.GetFewInactive(num)
	t.rwmtx.RUnlock()
	return res
}
