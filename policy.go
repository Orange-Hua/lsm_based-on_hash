package db

import (
	"kv/internal"
	"kv/lfu"
	"log"
	"sort"
)

const (
	MAXCOST   = 4096
	SAMPLENUM = 5
)

type Policy interface {
	tableToBuffer()
	diskToTable()
}
type lruManager interface {
	GetLowHotItems(int, bool) []internal.UniqeKey
	GetFewActive(int, bool) []internal.UniqeKey
	GetCurrentCapacity() uint
}
type policy struct {
	table         lruManager
	cache         lruManager
	disk          lruManager
	lfu           *lfu.TinyLFU
	tpolicyC      chan *internal.Chunk
	dpolicyC      chan *internal.Chunk
	cacheVictim   int
	tableActive   int
	tableInactive int
	diskActive    int
}

func newPolicy(
	cache, table, disk lruManager,
	lfu *lfu.TinyLFU,
	tpolicyC, dpolicyC chan *internal.Chunk,

) *policy {
	p := &policy{
		cache: cache,
		table: table,
		disk:  disk,

		lfu:           lfu,
		tpolicyC:      tpolicyC,
		dpolicyC:      dpolicyC,
		cacheVictim:   5,
		tableActive:   5,
		tableInactive: 5,
		diskActive:    10,
	}

	return p
}

type node struct {
	score int64
	key   internal.UniqeKey
}
type lessscores []int64
type greatscores []*node

func (s lessscores) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s lessscores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s lessscores) Len() int {
	return len(s)
}

func (s greatscores) Less(i, j int) bool {
	return s[i].score > s[j].score
}

func (s greatscores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s greatscores) Len() int {
	return len(s)
}

/*
1.如果cache空间足，所有规定个数的活跃值直接晋升
2.空间不足，计算table的最活跃，和cache最不活跃的比较分数，table从最活跃的项开始，找到该table分数，在caceh里小于该分数的最大索引
*/
func (p *policy) tableToBuffer() {

	actives := p.table.GetFewActive(p.tableActive, true)
	if len(actives) == 0 {
		log.Println("kv/db/policy/ tableToBuffer():table的活跃读为0,不晋升")
		return
	}
	//从buffer获取目标数量的key
	var res []internal.UniqeKey = actives
	cacheCapacity := p.cache.GetCurrentCapacity()
	if cacheCapacity > 80 {
		victimKeys := p.cache.GetLowHotItems(p.cacheVictim, true)

		if len(victimKeys) == 0 {
			log.Println("kv/db/policy.go--diskToTable():cache没有lru,写多读少，不晋升")
			return
		}
		res = p.findUpKeys(victimKeys, actives)
	}

	if len(res) == 0 {
		return
	}
	r := make([]*internal.Pair, 0, len(res))
	for i := range res {
		r = append(r, &internal.Pair{Key: internal.NewInternalKey(res[i].Key, res[i].ConflictKey, 0)})
	}
	p.tpolicyC <- &internal.Chunk{Data: r}
	log.Printf("kv/db/policy.go--tableToBuffer(): tableToBuffer上升了%d个\r\n", len(res))
	// if len(ukeys) == 0 {
	// 	return
	// }

	//p.tpolicyC <- ukeys

}

func (p *policy) findUpKeys(victimKeys, actives []internal.UniqeKey) []internal.UniqeKey {
	upScore := make(greatscores, len(actives))
	vicScore := make(lessscores, len(victimKeys))

	for i := range actives {
		upScore[i] = &node{score: p.lfu.Estimate(actives[i].Key), key: actives[i]}
	}
	for i := range victimKeys {
		vicScore[i] = p.lfu.Estimate(victimKeys[i].Key)
	}
	sort.Sort(upScore)
	sort.Sort(vicScore)
	res := []internal.UniqeKey{}

	index := sort.Search(len(vicScore), func(i int) bool {
		return upScore[0].score <= vicScore[i]
	})
	if index > 0 {
		index = index - 1
	}
	for v, u := index, 0; v >= 0 && u < len(actives); {
		if upScore[u].score > vicScore[v] {

			res = append(res, upScore[u].key)
			u++
		}
		v--

	}

	return res
}

func (p *policy) diskToTable() {

	actives := p.disk.GetFewActive(p.diskActive, true)
	//从buffer获取目标数量的key
	if len(actives) == 0 {
		log.Println("kv/db/policy/diskToTable()  disk的活跃读为0,不晋升")
		return
	}
	var res []internal.UniqeKey = actives
	tableCapacity := p.table.GetCurrentCapacity()
	if tableCapacity > 80 {
		ukeys := p.table.GetLowHotItems(p.tableInactive, true)

		if len(ukeys) == 0 {
			log.Println("kv/db/policy.go--diskToTable():table没有lru,写多读少，不晋升")
			return
		}
		res = p.findUpKeys(ukeys, actives)
	}

	if len(res) == 0 {
		return
	}
	r := make([]*internal.Pair, 0, len(res))
	for i := range res {
		r = append(r, &internal.Pair{Key: internal.NewInternalKey(res[i].Key, res[i].ConflictKey, 0)})
	}
	p.dpolicyC <- &internal.Chunk{Data: r}
	log.Printf("kv/db/policy.go--diskToTable():disk2table上升了%d个\r\n", len(res))
	// if len(ukeys) == 0 {
	// 	return
	// }

	// p.dpolicyC <- ukeys

}
