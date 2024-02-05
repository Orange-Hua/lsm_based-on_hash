package membuf

import (
	"kv/internal"
	"sync"
	"unsafe"
)

type policy struct {
	costs       map[internal.UniqeKey]uint64
	maxCapacity uint64
	lru         *bufLru
	curCost     uint64
	nums        int
	mutex       sync.RWMutex
}

func newPolicy(maxCapacity uint64, lru *bufLru) *policy {
	p := policy{maxCapacity: maxCapacity, lru: lru}
	p.costs = make(map[internal.UniqeKey]uint64, 256)
	p.curCost = 0
	return &p
}

func (p *policy) reviseCostMap(newJoin []*internal.Pair) {
	for _, v := range newJoin {
		p.costs[v.Key.UniqeKey] = uint64(v.Key.ValueLen)
	}

}
func calcuAddCost(newJoin []*internal.Pair) uint64 {
	var addc uint64 = 0
	for i := range newJoin {
		_ = i
		addc += uint64(unsafe.Sizeof(internal.UniqeKey{}))
		addc += uint64(unsafe.Sizeof(storeItem{}))
	}
	return addc
}

func (p *policy) categorizPairs(c *internal.Chunk) (newJoin []*internal.Pair) {
	for _, v := range c.Data {
		if _, ok := p.costs[v.Key.UniqeKey]; !ok {

			newJoin = append(newJoin, v)
		}
	}
	return
}
func (p *policy) judge(c *internal.Chunk, zone internal.Zone) (newJoin []*internal.Pair, victims []internal.UniqeKey, accept bool) {
	if zone != internal.TABLE {
		return newJoin, nil, false
	}
	newJoin = p.categorizPairs(c)
	addCost := calcuAddCost(newJoin)
	if addCost+p.curCost < p.maxCapacity {
		p.nums += len(newJoin)
		p.reviseCostMap(newJoin)
		return newJoin, nil, true
	}
	var sum uint64 = 0

	for {
		ukey := p.lru.Victim(false)
		cost := p.costs[ukey]
		delete(p.costs, ukey)
		sum += cost
		victims = append(victims, ukey)
		if sum > addCost {
			break
		}
	}
	return newJoin, victims, true
}
