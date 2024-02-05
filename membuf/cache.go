package membuf

import (
	"kv/internal"
	"log"
	"sync"
)

const defaultSegmentNumber = 64

type cache struct {
	interval       internal.Interval
	p              *policy
	lru            *bufLru
	readbufferSize int
	readKeyC       chan internal.UniqeKey //get cache时写入
	maxCapacity    uint64
	segments       [defaultSegmentNumber]segment
}

func NewMemBuffer(interval internal.Interval, maxCapacity uint64) *cache {
	c := cache{}
	c.maxCapacity = maxCapacity
	if c.readbufferSize == 0 {
		c.readbufferSize = 256
	}
	c.readKeyC = make(chan internal.UniqeKey, c.readbufferSize)

	c.lru = newBufLru(c.readKeyC)

	c.p = newPolicy(c.maxCapacity, c.lru)
	c.interval = interval

	return &c
}

func (c *cache) Get(k *internal.InternalKey) (*internal.Pair, bool) {
	ukey := k.UniqeKey

	index := getIndex(ukey.Key)
	//log.Printf("++++++++++++++++++++++cache读%d,part的索引是%d\r\n", k.Key, index)
	v, ok := c.segments[index].get(ukey)
	//log.Printf("------------------测试cache Get：key:%d,ckey:%d,结果:%t\r\n", ukey.Key, ukey.ConflictKey, ok)
	if ok {
		select {
		case c.readKeyC <- ukey:
			//log.Println("kv/membuffer/cache.go/Get():从cache读数据，已经写进lruBuffer")
		default:
		}
	}
	return v, ok

}

func (c *cache) MultiPut(pairs []*internal.Pair) {
	for _, v := range pairs {
		c.Put(v)
	}

}

func (c *cache) Put(pair *internal.Pair) {

	ikey := pair.Key
	index := getIndex(ikey.Key)
	//log.Printf("++++++++++++++++++++++cache写%d,part的索引是%d\r\n", pair.Key.Key, c.index)
	c.segments[index].put(pair)
}

func (c *cache) delete(ukey internal.UniqeKey) {

	index := getIndex(ukey.Key)
	c.segments[index].del(ukey)

}

func getIndex(key uint64) uint64 {
	index := key % defaultSegmentNumber
	return index
}

func (c *cache) put(chunk *internal.Chunk, newJoin []*internal.Pair, victims []internal.UniqeKey, accept bool) {

	if !accept {
		//log.Printf("kv/cache.go--put():数据不被cache接收，流入table\r\n")

		return
	}
	log.Printf("kv/cache.go--put():数据被cache接收\r\n")
	if victims != nil {
		log.Printf("kv/cache.go--put():数据被cache接收，内存不足驱逐数据\r\n")
		for i := 0; i < len(victims); i++ {
			c.delete(victims[i])
		}

	}
	//log.Printf("kv/cache.go--put():数据被cache接收，插入新数据\r\n")
	c.MultiPut(newJoin)

}

func (c *cache) PutFrom(upC chan *internal.Chunk) {
	go func() {
		for {
			select {
			case chunk := <-upC:
				log.Println("kv/membuf/cache.go--Run():接收到table晋升数据")
				newJoin, victims, accept := c.p.judge(chunk, internal.TABLE)
				if len(newJoin) == 0 {
					continue
				}
				c.put(chunk, newJoin, victims, accept)
			}
		}
	}()
}

func (c *cache) Statistic() {
	log.Printf("kv/membuf/policy.go--Statistic():当期容量%d，当前存储的元素个数%d\r\n", c.p.curCost, c.p.nums)
}
func (c *cache) GetLowHotItems(num int, sync bool) []internal.UniqeKey {
	return c.lru.GetFewInactive(num, sync)
}
func (c *cache) GetFewActive(num int, sync bool) []internal.UniqeKey {
	return c.lru.GetFewActive(num, sync)
}

func (c *cache) GetCurrentCapacity() uint {
	return uint(c.p.curCost / c.p.maxCapacity * 100)
}

type storeItem struct {
	seq   uint64
	value any
}

type segment struct {
	rw     sync.RWMutex
	bucket sync.Map
}

/*
cache相当于shardedMap

	segment相当于 lockedmap
*/
func (s *segment) put(p *internal.Pair) {

	_, ok := s.bucket.Load(p.Key.UniqeKey)
	if !ok {
		s.bucket.Store(p.Key.UniqeKey, p)
		log.Printf("cache put:key:%d,ckey:%d值是:%s\r\n", p.Key.Key, p.Key.ConflictKey, string(p.Value))
		return
	} else {
		log.Printf("kv/cache/segment.put()--cache不存在put更新操作\r\n")
	}

}

func (s *segment) del(ukey internal.UniqeKey) (any, bool) {
	return s.bucket.LoadAndDelete(ukey)

}

func (s *segment) get(ukey internal.UniqeKey) (*internal.Pair, bool) {

	v, ok := s.bucket.Load(ukey)
	if ok {
		return v.(*internal.Pair), ok
	}
	return nil, ok
}
