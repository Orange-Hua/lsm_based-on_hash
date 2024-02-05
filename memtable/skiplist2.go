package memtable

import (
	"fmt"
	"kv/config"
	"kv/fc"
	"kv/internal"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type vNode struct {
	skHead *SkipList
	next   *vNode
	pre    *vNode
}

type Table struct {
	p        policy
	getFrom  chan *internal.Chunk
	victimTo chan *internal.Chunk
	// upKeysToCache  chan *internal.Chunk
	// upKeysFromDisk chan *internal.Chunk
	tpolicyC chan *internal.Chunk
	fc       *fc.FlowCtl
	// pendingChunk *list.List
	// immi         atomic.Bool
	//diskComplete chan uintptr
	lru         *tableLru
	versionC    chan struct{}
	maxLevel    int
	maxCapacity int
	cmp         internal.Comparer
	readBuffer  chan internal.UniqeKey
	interval    *internal.Interval

	versionHead  *vNode
	versionTail  *vNode
	versionNum   atomic.Int32
	versionSeq   int
	versionMutex sync.RWMutex
	gpool        *internal.GPool
	// diskNotify   chan diskN
	immu atomic.Bool

	immuTableNum int
	diskFunc     func(*internal.Chunk)

	upKeysFromDisk chan *internal.Chunk

	Skiplist *SkipList
}

type Options struct {
	keyCmp   func(any, any) int
	maxLevel int
	locker   sync.Locker
}

type Option func(*Options)

type Element struct {
	Node
	p *internal.Pair
}
type Node struct {
	next []*Element
}

type SkipList struct {
	cmp internal.Comparer
	//ucmp     internal.UCmp
	maxLevel int

	level int

	head   *Element
	tail   *Element
	rander *rand.Rand
}

func (p *Table) Init() {

	//p.immi.Store(false)
	//atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&p.head)), unsafe.Pointer(unsafe.Pointer(head)))

	p.lru.reset()
	p.p.reset()
}

func NewMemTable(
	fc *fc.FlowCtl,
	interval *internal.Interval,
	topt *config.TableOpt,
	diskFunc func(*internal.Chunk),
) *Table {

	t := &Table{
		cmp:         internal.DefaultComparer,
		maxLevel:    topt.MaxLevel,
		maxCapacity: int(topt.MaxCapacity),

		fc: fc,
	}
	// t.immuTableNum = 4
	// if topt != nil {
	// 	t.immuTableNum = topt.ImmuTableNum
	// }

	// t.interval = interval

	// t.tpolicyC = make(chan *internal.Chunk, 8)
	t.readBuffer = make(chan internal.UniqeKey, 256)
	// t.versionC = make(chan struct{}, t.immuTableNum)
	// for i := 0; i < t.immuTableNum; i++ {
	// 	t.versionC <- struct{}{}
	// }
	// //t.diskComplete = make(chan uintptr, 1)

	t.lru = newLru(t.readBuffer)
	// t.versionHead = new(vNode)
	t.p = *newPolicy(uint64(t.maxCapacity), uint64(t.maxCapacity), t.lru)
	// v := new(vNode)
	//sk := NewSkipList(t.maxLevel, t.cmp)
	//t.Skiplist = sk
	// v.skHead = sk
	// t.versionHead.next = v
	// v.pre = t.versionHead
	// tail := new(vNode)
	// v.next = tail
	// tail.pre = v
	// t.versionTail = tail
	// t.diskFunc = diskFunc
	// t.gpool = internal.NewGPool(8, internal.Asy)
	t.Init()
	//t.Run()
	return t
}

func (t *Table) NewSkipList(
	maxLevel int,
	cmp internal.Comparer,
) *SkipList {
	head := new(Element)
	tail := new(Element)
	head.next = make([]*Element, maxLevel, maxLevel)
	for i := 0; i < maxLevel; i++ {
		head.next[i] = tail
	}
	sl := &SkipList{
		cmp:      cmp,
		maxLevel: maxLevel,
		//maxCapacity: int(maxCapacity),
		//diskStart:     diskStart,
		//diskCompleteC: diskComplete,
		head: head,
		tail: tail,

		rander: rand.New(rand.NewSource(time.Now().Unix())),
	}
	t.Skiplist = sl
	return sl
}
func NewElement(key *internal.InternalKey, value []byte, level int) *Element {
	node := Node{next: make([]*Element, level)}

	e := &Element{p: &internal.Pair{Key: key, Value: value}, Node: node}

	return e
}

func (sl *SkipList) RandomLevel() int {
	total := uint64(1)<<uint64(sl.maxLevel) - 1 // 2^n-1
	k := sl.rander.Uint64() % total
	levelN := uint64(1) << (uint64(sl.maxLevel) - 1)

	level := 1
	for total -= levelN; total > k; level++ {
		levelN >>= 1
		total -= levelN
	}
	return level
}

/*
preds: 前继节点
succs: 后继结点
*/
func (sl *SkipList) Herlihy_find_from_preds(key *internal.InternalKey, preds, succs []*Element, level int) (hit bool) {
	//原地更新pred

	curLevel := level
	pred := preds[curLevel]

	var n *Element
	for l := curLevel; l >= 0; l-- {
		for {
			n = (*Element)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pred.next[l]))))
			if n != sl.tail && sl.cmp.CompareUniqeKey((&n.p.Key.UniqeKey), &key.UniqeKey) < 0 {
				//fmt.Println(n.p.Key)
				pred = n
				continue
			}
			preds[l] = pred
			succs[l] = n
			break
		}
	}

	return n != sl.tail && sl.cmp.CompareUniqeKey(&n.p.Key.UniqeKey, &key.UniqeKey) == 0
}

func (sl *SkipList) insert(p *internal.Pair, preds []*Element) {
	var (
		newNode   *Element
		numInsert int
	)

	level := sl.RandomLevel()
	index := level - 1
	// log.Printf("key:%d,level:%d", key, level)

	newNode = NewElement(p.Key, p.Value, level)
	succs := make([]*Element, sl.maxLevel)

	for {
		//fmt.Println("进入insert的for循环")
		if hit := sl.Herlihy_find_from_preds(p.Key, preds, succs, index); hit {
			//fmt.Println("进入insert的更新")
			oldValue := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&(succs[0].p))))
			if p.Key.SeqNum < (*internal.Pair)(oldValue).Key.SeqNum {
				break
			}
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&(succs[0].p))), oldValue, unsafe.Pointer(p)) {
				//str := fmt.Sprintf("%v", succs[0].p.Value)
				//fmt.Println(str)

				break
			}
		} else {
			//fmt.Println("进入insert的for循环else分支")
			for i := 0; i < level; i++ {
				newNode.next[i] = succs[i]
			}

			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&preds[0].next[0])), unsafe.Pointer(succs[0]), unsafe.Pointer(newNode)) {
				// sl.rwmux.Lock()
				// if level > sl.level {
				// 	sl.level = level
				// }
				// sl.rwmux.Unlock()
				sl.Herlihy_find_from_preds(p.Key, preds, succs, index)
				for i := 1; i < level; i++ {

					for {

						if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&preds[i].next[i])), unsafe.Pointer(succs[i]), unsafe.Pointer(newNode)) {
							break
						} else {
							sl.Herlihy_find_from_preds(p.Key, preds, succs, index)
						}
					}
				}
				numInsert++

				break
			}
		}
	}
}

func (p *Table) putChunk(entries *internal.Chunk) {
	if len(entries.Data) == 0 {
		return
	}
	sort.Sort(entries)
	sl := p.Skiplist
	preds := make([]*Element, sl.maxLevel)

	for i := 0; i < sl.maxLevel; i++ {
		preds[i] = sl.head
	}
	for _, entry := range entries.Data {

		sl.insert(entry, preds)
		//fmt.Printf("第%d个插入了第%d个\r\n", index, i)
	}

}

// func (p *Table) putPair(pair *internal.Pair) {
// 	sl := p.currentVersion().skHead
// 	preds := make([]*Element, sl.maxLevel)
// 	for i := 0; i < sl.maxLevel; i++ {
// 		preds[i] = sl.head
// 	}
// 	sl.insert(pair, preds)
// }

func (sl *SkipList) Range(from *internal.InternalKey, to *internal.InternalKey) ([]any, []*internal.Pair) {

	if sl.cmp.CompareUniqeKey(&to.UniqeKey, &from.UniqeKey) < 0 {
		return nil, nil
	}
	preds := make([]*Element, sl.maxLevel)
	for i := 0; i < sl.maxLevel; i++ {
		preds[i] = sl.head
	}
	succs := make([]*Element, sl.maxLevel)
	sl.Herlihy_find_from_preds(from, preds, succs, sl.maxLevel-1)
	f := preds[0]

	sl.Herlihy_find_from_preds(to, preds, succs, sl.maxLevel-1)
	t := preds[0]
	ptr := f
	var (
		res   []any
		pairs []*internal.Pair
	)
	for uintptr(unsafe.Pointer(t)) != uintptr(unsafe.Pointer(ptr)) {
		next := (*Element)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ptr.next[0]))))
		if next != sl.tail && sl.cmp.CompareUniqeKey(&next.p.Key.UniqeKey, &t.p.Key.UniqeKey) <= 0 {
			res = append(res, next.p.Value)
			pairs = append(pairs, next.p)
			ptr = next
		}

	}

	if uintptr(unsafe.Pointer(t.next[0])) != uintptr(unsafe.Pointer(sl.tail)) && sl.cmp.CompareUniqeKey(&t.next[0].p.Key.UniqeKey, &to.UniqeKey) == 0 {
		res = append(res, t.p.Value)
		pairs = append(pairs, t.p)
	}

	return res, pairs
}

func (sl *SkipList) Scan1() {
	level := sl.maxLevel - 1
	n := 0
	w, err := os.OpenFile("f:\\kv\\data\\table1.txt", os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		log.Fatalf("创建文件失败\r\n")
	}
	defer w.Close()
	for i := level; i >= 0; i-- {

		fmt.Fprintf(w, "第%d行\r\n", i)
		ptr := sl.head
		for ptr.next[i] != sl.tail {
			fmt.Fprintf(w, "%d    ", ptr.next[i].p.Key.Key)
			if i == 0 {
				n++
			}
			ptr = ptr.next[i]
		}

		//fmt.Printf("%d ", PointerToElePtr(&ptr).key)
		fmt.Fprintf(w, "\r\n")
	}
	fmt.Fprintf(w, "总数是%d\r\n", n)
}

func (sl *Table) ChooseKeyToUp(c, upKeysToCache chan *internal.Chunk) {
	go func() {
		record := make(map[internal.UniqeKey]int, 16)
		dup := make([]internal.UniqeKey, 0, 4)
		first := false
		for upchunk := range c {
			if !first {
				first = true
			}
			var pairs []*internal.Pair
			for i := range upchunk.Data {
				if _, ok := record[upchunk.Data[i].Key.UniqeKey]; ok {
					dup = append(dup, upchunk.Data[i].Key.UniqeKey)

					continue
				}
				record[upchunk.Data[i].Key.UniqeKey] = 1

				entryAddr := sl.lru.GetValueAddr(upchunk.Data[i].Key.UniqeKey)
				pairs = append(pairs, entryAddr.p)
				entryAddr.p.Key.Kind = internal.InternalKeyKindUpBuffer

			}
			if len(pairs) > 0 {
				upKeysToCache <- &internal.Chunk{Data: pairs}
			}
			if !first {
				for uk := range record {
					delete(record, uk)
				}
			}

			for i := 0; i < len(dup); i++ {
				record[dup[i]] = 1
			}
			dup = make([]internal.UniqeKey, 4)
		}
	}()
}
func (t *Table) Get(key *internal.InternalKey) ([]byte, bool) {

	v, ok := t.Skiplist.Get(key)
	if ok {
		t.lru.Put(key.UniqeKey)

	}
	return v, ok
}
func (sl *SkipList) Get(key *internal.InternalKey) ([]byte, bool) {

	preds := make([]*Element, sl.maxLevel)

	if sl == nil {
		log.Fatalf("kv/memtable/skiplist2.go--Get():table的head是nil\r\n")
	}
	for i := 0; i < sl.maxLevel; i++ {
		preds[i] = sl.head
	}
	succs := make([]*Element, sl.maxLevel)

	hit := sl.Herlihy_find_from_preds(key, preds, succs, sl.maxLevel-1)

	if hit {
		p := succs[0].p
		if p.Key.Kind == internal.InternalKeyKindDelete {
			return nil, false
		}

		return p.Value, true
	}
	return nil, false
}

// func (sl *Table) Get(key *internal.InternalKey) ([]byte, bool) {
// 	sl.versionMutex.RLock()
// 	defer sl.versionMutex.RUnlock()
// 	ptr := sl.versionHead
// 	for {
// 		node := ptr.next
// 		if node == sl.versionTail {
// 			return nil, false
// 		}
// 		preds := make([]*Element, sl.maxLevel)
// 		sk := node.skHead
// 		if sk == nil {
// 			log.Fatalf("kv/memtable/skiplist2.go--Get():table的head是nil\r\n")
// 		}
// 		for i := 0; i < sl.maxLevel; i++ {
// 			preds[i] = sk.head
// 		}
// 		succs := make([]*Element, sl.maxLevel)

// 		hit := sk.Herlihy_find_from_preds(key, preds, succs, sl.maxLevel-1)

// 		if hit {
// 			p := succs[0].p
// 			if p.Key.Kind == internal.InternalKeyKindDelete {
// 				return nil, false
// 			}
// 			sl.lru.PutAddr(key.UniqeKey, succs[0])
// 			return p.Value, true
// 		}
// 		ptr = node

// 	}

// }

func (sl *Table) GetLowHotItems(num int, sync bool) []internal.UniqeKey {

	r := sl.lru.GetFewInactive(num, sync)

	return r
}

func (sl *Table) GetFewActive(num int, sync bool) []internal.UniqeKey {

	r := sl.lru.GetFewActive(num, sync)
	return r

}

func (sl *Table) GetCurrentCapacity() uint {
	sum := 0
	return uint(sum)
}

func (sl *Table) Statistic() {
	log.Printf("kv/memtable/policy.go:当期容量%d，当前存储的元素个数%d\r\n", sl.p.curCapacity, sl.p.nums)
}

// func (sl *Table) disk(node *vNode) {
// 	from, to := sl.interval.GetData()
// 	f, t := internal.NewInternalKey(from, 0, 0), internal.NewInternalKey(to, math.MaxUint64, 0)
// 	_, diskPair := node.skHead.Range(f, t)

// 	chunk := &internal.Chunk{Index: sl.versionSeq, Data: diskPair, Smallest: f, Largest: t, Cmp: internal.DefaultComparer, Callback: func() {
// 		sl.diskComplete(node)
// 	}}
// 	sl.versionSeq++
// 	sl.diskFunc(chunk)
// 	//sl.fc.VictimToDisk <- chunk
// 	// sl.writeMemTable(chunk, func() {
// 	// 	sl.diskComplete(node)
// 	// })
// }

// func (sl *Table) diskComplete(node *vNode) {
// 	sl.deleteVersionNode(node)
// 	sl.versionNum.Add(-1)
// 	sl.versionC <- struct{}{}
// 	//log.Printf("table immuTable落盘回调，version数目是:%d", sl.versionNum.Load())
// 	sl.fc.StatisticPos.Store(sl.versionNum.Load())
// 	//sl.fc.Notify <- fc.Msg{Typ: uint8(1), Pos: uint8(sl.versionNum.Load()), Code: uint8(1)}
// 	//log.Println("完成后发fc通知成功")
// }

type diskN struct {
	vNodeAddr *vNode
	extra     *internal.Chunk
}

func (sl *Table) put(c *internal.Chunk, zone internal.Zone) (bool, bool) {
	// if sl.immi.Load() == true {
	// 	//log.Printf("kv/skiplist2.go--run():写盘阶段，数据%d直接进pending\r\n", c.Data[0].Key.Key)
	// 	sl.pendingChunk.PushBack(c)
	// 	return false
	// }
	if len(c.Data) == 0 {
		return false, false
	}
	update, newJoin, accpet := sl.p.judge(c, zone)

	sl.putChunk(update)
	if accpet == 1 {

		sl.putChunk(newJoin)
		//log.Printf("kv/skiplist2.go--run():%d被Table%d接收\r\n", newJoin.Data[0].Key.Key, sl.index)
		return true, true
	}
	if accpet == -1 {
		return false, false
	}
	if accpet != 1 && zone == internal.DISK {
		return false, false
	}

	//log.Printf("kv/skiplist2.go--run():写盘阶段，数据%d触发了写盘\r\n", newJoin.Data[0].Key.Key)
	//log.Printf("version数是%d\r\n", sl.immuTableNum)
	// <-sl.versionC
	// //log.Printf("put通过熔断\r\n")
	// curr := sl.currentVersion()
	// sl.currentVersionToImmuTable()
	// sl.versionNum.Add(1)
	// sl.fc.StatisticPos.Store(sl.versionNum.Load())
	//sl.fc.Notify <- fc.Msg{Typ: uint8(2), Pos: uint8(sl.versionNum.Load()), Code: uint8(1)}
	//log.Printf("put通过notify\r\n")
	// sl.init()

	// //sl.diskNotify <- diskN{vNodeAddr: curr, extra: extra}
	// //log.Printf("kv/skiplist2.go--run():写盘阶段，Table%d数据%d触发了写盘\r\n", sl.index, newJoin.Data[0].Key.Key)

	// //sl.disk(curr)
	// //log.Println("table落盘完成")
	// fun := sl.disk
	// sl.gpool.AsyPut(func() {
	// 	fun(curr)
	// })
	//log.Printf("kv/skiplist2.go--run():执行完写盘\r\n")
	return true, false
}

func (t *Table) Put(c *internal.Chunk) bool {

	accept, succ := t.put(c, internal.BUFFER)
	if accept && succ {
		//log.Println(i)
		//log.Println("table写入一个数据")
		return true

	}
	return false
}

func (p *Table) Run() error {

	go func() {

		for {
			select {
			case c := <-p.fc.Db2table:
				p.Put(c)

				//fmt.Printf("table接收了数据%d成功\r\n", c.Data[0].Key.Key)
			}
		}

	}()
	go func() {
		defer fmt.Println("skip run结束")

		for {
			select {

			// case vuintptr := <-p.diskComplete:
			// 	vNodeAddr := (*vNode)(unsafe.Pointer(vuintptr))
			// 	p.deleteVersionNode(vNodeAddr)

			case <-p.upKeysFromDisk:

				//p.put(c, internal.DISK)
				//log.Println("kv/memtable/skiplist2.go--Run():从disk接收到晋升数据")
			}
		}
	}()
	return nil
}
