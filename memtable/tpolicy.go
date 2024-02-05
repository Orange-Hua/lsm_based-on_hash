package memtable

import (
	"kv/internal"
	"sync"
	"sync/atomic"
	"unsafe"
)

type policy struct {
	totalMaxCapacity uint64
	partMaxCapacity  uint64
	curCapacity      uint64
	nums             int
	lru              *tableLru
	costs            map[internal.UniqeKey]uint64
	mutex            sync.RWMutex
	totalCapacity    *[]atomic.Uint64
}

func newPolicy(partMaxCapacity, totalMaxCapacity uint64, lru *tableLru) *policy {
	p := policy{
		partMaxCapacity:  partMaxCapacity,
		totalMaxCapacity: totalMaxCapacity,
		lru:              lru,
		curCapacity:      0,
	}
	p.costs = make(map[internal.UniqeKey]uint64, 256)

	return &p

}

func calcuCost(update, newJoin *internal.Chunk) uint64 {
	addc := uint64(unsafe.Sizeof(internal.Pair{}))
	for _, p := range newJoin.Data {
		addc += uint64(unsafe.Sizeof(*p.Key))
		addc += p.Key.ValueLen
	}
	return addc
}

func (p *policy) categorizPairs(c *internal.Chunk) (update, newJoin *internal.Chunk) {
	var (
		u, n []*internal.Pair
	)
	for _, pair := range c.Data {

		if _, ok := p.costs[pair.Key.UniqeKey]; ok {
			u = append(u, pair)
		} else {
			n = append(n, pair)
		}
	}
	return &internal.Chunk{Data: u}, &internal.Chunk{Data: n}
}

// 修改cost map，加入新增元素尺寸
func (p *policy) reviseCostMap(update, newJoin []*internal.Pair) {
	for _, v := range newJoin {
		p.costs[v.Key.UniqeKey] = v.Key.ValueLen
	}

}
func (p *policy) getTotalCapacity() uint64 {

	return p.curCapacity
}

func (p *policy) judge(c *internal.Chunk, zone internal.Zone) (update, newJoin *internal.Chunk, accpet int) {
	update, newJoin = p.categorizPairs(c)
	addc := calcuCost(update, newJoin)

	if zone == internal.BUFFER {
		desireSize := p.curCapacity + addc
		curTotalCapacity := p.getTotalCapacity()
		if addc > p.partMaxCapacity {
			return update, newJoin, -1
		}
		if desireSize < p.partMaxCapacity || desireSize >= p.partMaxCapacity && curTotalCapacity+addc < p.totalMaxCapacity {

			p.curCapacity += addc
			p.nums += len(newJoin.Data)
			p.reviseCostMap(update.Data, newJoin.Data)
			return update, newJoin, 1
		}

		return update, newJoin, 2
	}
	if zone == internal.DISK {
		return update, newJoin, 1
		desireSize := p.curCapacity + addc
		curTotalCapacity := p.getTotalCapacity()
		if desireSize < p.partMaxCapacity || desireSize >= p.partMaxCapacity && curTotalCapacity+addc < p.totalMaxCapacity {
			p.curCapacity += addc

			p.nums += len(newJoin.Data)
			p.reviseCostMap(update.Data, newJoin.Data)
			return update, newJoin, 1
		}
		return update, newJoin, -1
	}
	return nil, nil, -1
}

func (p *policy) reset() {

	p.costs = make(map[internal.UniqeKey]uint64, 256)
	p.curCapacity = 0
	p.nums = 0
}
