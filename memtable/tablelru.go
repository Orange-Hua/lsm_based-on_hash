package memtable

import (
	"kv/internal"
	"sync"
)

// 目前晋升时用
type tableLru struct {
	readBuffer chan internal.UniqeKey
	s          internal.SegmentLru
	addrmap    map[internal.UniqeKey]*Element
	rwmtx      sync.RWMutex
}

func newLru(r chan internal.UniqeKey) *tableLru {
	b := tableLru{}
	b.s = internal.NewSegmentLru(32)
	b.readBuffer = r
	b.addrmap = make(map[internal.UniqeKey]*Element, 64)
	return &b
}
func (t *tableLru) PutAddr(key internal.UniqeKey, nodeAddr *Element) {
	t.rwmtx.Lock()
	t.addrmap[key] = nodeAddr
	t.rwmtx.Unlock()
}

func (t *tableLru) Put(key internal.UniqeKey) {
	t.s.Put(key)

}
func (t *tableLru) Sync() {
	t.rwmtx.Lock()
	n := len(t.readBuffer)
	for i := 0; i < n; i++ {
		t.s.Put(<-t.readBuffer)
	}
	t.rwmtx.Unlock()
}

func (t *tableLru) GetFewActive(limit int, sync bool) []internal.UniqeKey {

	if sync {
		t.Sync()
	}
	t.rwmtx.RLock()
	res := t.s.GetFewActive(limit)
	t.rwmtx.RUnlock()
	return res
}
func (t *tableLru) GetFewInactive(num int, sync bool) []internal.UniqeKey {
	if sync {
		t.Sync()
	}
	t.rwmtx.RLock()
	res := t.s.GetFewInactive(num)
	t.rwmtx.RUnlock()
	return res
}
func (t *tableLru) GetVictim(sync bool) internal.UniqeKey {
	if sync {
		t.Sync()
	}
	t.rwmtx.RLock()
	res := t.s.GetVictim()
	t.rwmtx.RUnlock()
	return res
}

func (t *tableLru) GetValueAddr(ukey internal.UniqeKey) *Element {
	t.rwmtx.RLock()
	defer t.rwmtx.RUnlock()
	return t.addrmap[ukey]
}

func (t *tableLru) reset() {
	t.s = internal.NewSegmentLru(internal.LRUACTIVENUM)
	t.addrmap = make(map[internal.UniqeKey]*Element, 64)
}
