package db

import (
	"kv/config"
	"kv/fc"
	"kv/internal"
	"kv/lfu"
	"kv/membuf"
	"kv/memtable"
	"kv/storage"
	"kv/utils"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type MemBuffer interface {
	Get(*internal.InternalKey) (*internal.Pair, bool)
	// Put(*internal.Pair)
	// MultiPut([]*internal.Pair)
	//run(int, chan *internal.Chunk, chan *internal.Chunk, chan *internal.Chunk)
	PutFrom(chan *internal.Chunk)
	//StartPart(int, chan *internal.Chunk, chan *internal.Chunk, chan *internal.Chunk) internal.Part
}

type MemTable interface {
	Get(*internal.InternalKey) ([]byte, bool)
	Put(*internal.Chunk) bool
	//MultiPut(*internal.Chunk)
	NewSkipList(int, internal.Comparer) *memtable.SkipList
	Init()
	//StartPart(int, chan *internal.Chunk, chan *internal.Chunk, chan *internal.Chunk) internal.Part
	ChooseKeyToUp(chan *internal.Chunk, chan *internal.Chunk)
	//run()
}

type Storage interface {
	Get(*internal.InternalKey) ([]byte, bool, error)
	//Put(*internal.Chunk)
	ChooseKeyToUp(chan *internal.Chunk)
	Compact() error
	DiskTable(chunk *internal.Chunk)
	WriteMemTable(chunk *internal.Chunk, callback func())
	//StartPart(int, chan *internal.Chunk, chan *internal.Chunk) internal.Part
	//WriteLog(key *internal.InternalKey, value []byte) error
}

type DB struct {
	interval *internal.Intervals
	parts    []chan *internal.Chunk
	ps       []*Part
	stopC    chan struct{}
	seq      atomic.Uint64
}

func NewDB(opts *config.Options) *DB {
	interval := opts.Interval
	intervalNum := len(interval)
	parts := make([]chan *internal.Chunk, intervalNum)
	ps := make([]*Part, intervalNum)
	for i := 0; i < intervalNum; i++ {
		parts[i] = make(chan *internal.Chunk, N)
	}
	db := &DB{
		interval: &internal.Intervals{Data: interval},
		parts:    parts,
		ps:       ps,
	}
	for i := 0; i < intervalNum; i++ {
		p := db.StartPart(i, parts[i], opts)
		db.ps[i] = p
		go p.run()
	}
	return db
}

func (db *DB) StartPart(index int, dataC chan *internal.Chunk, opts *config.Options) *Part {
	p := NewPart(index, dataC, opts)
	return p
}
func (db *DB) Get(key any) ([]byte, bool) {
	k, _ := utils.KeyToHash(key)
	ikey := internal.NewInternalKey(k, 0, 0)
	index, _ := db.interval.GetIndex(ikey.Key)
	return db.ps[index].Get(ikey)
}

type PairPool struct {
	pairs []*internal.Pair
	fc    *fc.FlowCtl
}

func newPairPool(fc *fc.FlowCtl) *PairPool {
	p := &PairPool{}
	p.pairs = make([]*internal.Pair, 0, 40960)
	p.fc = fc
	return p
}

func (p *PairPool) Put(pair *internal.Pair) {
	p.pairs = append(p.pairs, pair)
	if len(p.pairs) == cap(p.pairs) {
		p.PopAllData()
		p.pairs = make([]*internal.Pair, 0, 40960)
	}
}

func (p *PairPool) PopAllData() {
	chunk := internal.Chunk{Data: p.pairs}
	//<-part.versionC
	//log.Println("进流控")
	if p.fc.Ctling.Load() {
		<-p.fc.Signal
	}
	dbC := p.fc.GetFromDB
	if dbC != nil {
		dbC <- &chunk
	} else {
		<-p.fc.Signal
	}
}
func (db *DB) Put(key any, value []byte, cost uint64) {

	//process线程池选第几个线程

	//放进去

	k, _ := utils.KeyToHash(key)
	seq := db.seq.Add(1)
	ikey := internal.NewInternalKey(k, 0, seq)
	ikey.Kind = internal.InternalKeyKindSet
	ikey.ValueLen = cost

	// if err := db.disk.WriteLog(ikey, value); err != nil {
	// 	return
	// }
	pair := internal.Pair{Key: ikey, Value: value}
	index, _ := db.interval.GetIndex(ikey.Key)
	part := db.ps[index]
	part.pairPool.Put(&pair)
	// if v, ok := db.memBuffer.Get(ikey); ok {
	// 	if _, ok := db.updateCache(&pair, v); ok {
	// 		return
	// 	}
	// }
	//chunk := internal.Chunk{Data: []*internal.Pair{&pair}}

	//<-part.versionC
	// //log.Println("进流控")
	// if part.fc.Ctling.Load() {
	// 	<-part.fc.Signal
	// }
	// dbC := part.fc.GetFromDB
	// if dbC != nil {
	// 	dbC <- &chunk
	// } else {
	// 	<-part.fc.Signal
	// }
	//db.parts[index] <- &chunk
	//db.ps[index].fc.Cond.L.Lock()
	//db.ps[index].fc.GetFromDB <- &chunk
	//log.Println("进入dbbufferC")
}

func (db *DB) Delete(key any) any {
	k, c := utils.KeyToHash(key)
	seq := db.seq.Add(1)
	ikey := internal.NewInternalKey(k, c, seq)
	ikey.Kind = internal.InternalKeyKindDelete
	pair := internal.Pair{Key: ikey, Value: nil}
	chunk := internal.Chunk{Data: []*internal.Pair{&pair}}
	index, _ := db.interval.GetIndex(ikey.Key)
	db.parts[index] <- &chunk
	return nil
}

func (db *DB) Open(dirname string, option internal.Options) error {
	return nil
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) init() {

}

func (db *DB) Wait() {
	<-db.stopC
}

func (db *DB) Compact() error {
	//return db..Compact()
	return nil
}
func (db *DB) Statistic() {
	// db.memBuffer.Statistic()
	// db.memTable.Statistic()
}

var N = 1024

func NewDirectionTableCacheC(correntNum int) chan *internal.Chunk {

	ptr2 := make(chan *internal.Chunk, N)

	return ptr2

}

func NewTableUpgradeC(correntNum int) []chan *internal.Chunk {

	var tp = make([]chan *internal.Chunk, correntNum)
	var ptr1 chan *internal.Chunk
	for i := 0; i < correntNum; i++ {
		ptr1 = make(chan *internal.Chunk, N)
		tp[i] = ptr1
	}
	return tp

}
func NewBiDirectionTableDiskC(correntNum int) (chan *internal.Chunk, chan *internal.Chunk) {
	var (
		td = make(chan *internal.Chunk, correntNum)
		dt = make(chan *internal.Chunk, correntNum)
	)

	return td, dt

}
func NewDiskUpgradeC(correntNum int) []chan *internal.Chunk {

	var dp = make([]chan *internal.Chunk, correntNum)
	var ptr1 chan *internal.Chunk
	for i := 0; i < correntNum; i++ {
		ptr1 = make(chan *internal.Chunk, N)
		dp[i] = ptr1
	}
	return dp

}

func NewDbTableC(correntNum int) chan *internal.Chunk {

	ptr1 := make(chan *internal.Chunk, N)

	return ptr1

}

type Part struct {
	index    int
	opts     *config.Options
	fc       *fc.FlowCtl
	interval *internal.Interval
	MemBuffer
	MemTable
	Storage
	internal.Interval
	lfu       *lfu.TinyLFU
	lfuBuffer *internal.RingBuffer
	p         Policy
	mutex     sync.Mutex
	dataC     chan *internal.Chunk
	seq       atomic.Uint64
	stopC     chan struct{}

	versionHead  *vNode
	versionTail  *vNode
	versionNum   atomic.Int32
	versionSeq   int
	versionMutex sync.RWMutex
	gpool        *internal.GPool
	versionC     chan struct{}

	pairPool *PairPool
}
type vNode struct {
	skHead *memtable.SkipList
	next   *vNode
	pre    *vNode
}

func NewPart(index int, dataC chan *internal.Chunk, opts *config.Options) *Part {
	p := &Part{}
	lfuCounter := lfu.NewTinyLFU(opts.NumCounters)
	p.opts = opts
	if opts.Interval == nil {
		//interval = [][2]uint64{{0, 9999999999999999999}, {10000000000000000000, math.MaxUint64}}
		log.Fatalf("添加分片区间\r\n")

	}

	p.fc = fc.NewFlowCtl(1024, opts.LowLimit, opts.GeneralLimit, opts.HighLimit)
	p.interval = &internal.Interval{Data: opts.Interval[index]}
	table2CacheC := NewDirectionTableCacheC(N)
	tpolicyC, dpolicyC := make(chan *internal.Chunk, N), make(chan *internal.Chunk, 64)

	cache := membuf.NewMemBuffer(*p.interval, opts.Copt.MaxCapacity)
	//td, dt := NewBiDirectionTableDiskC(N)

	disk := storage.NewStorage(p.fc, p.interval, opts.Sopt.Dir+strconv.Itoa(index), &opts.Sopt)
	table := memtable.NewMemTable(p.fc, p.interval, &opts.Topt, disk.DiskTable)
	p.MemBuffer = cache
	p.MemTable = table
	p.Storage = disk
	p.dataC = dataC
	p.Storage.ChooseKeyToUp(dpolicyC)
	p.MemTable.ChooseKeyToUp(tpolicyC, table2CacheC)
	p.p = newPolicy(cache, table, disk, lfuCounter, tpolicyC, dpolicyC)

	p.lfu = lfuCounter
	p.lfuBuffer = internal.NewRingBuffer(p.lfu, int64(N))
	p.stopC = make(chan struct{})

	immuTableNum := p.opts.Topt.ImmuTableNum
	p.versionC = make(chan struct{}, immuTableNum)
	for i := 0; i < immuTableNum; i++ {
		p.versionC <- struct{}{}
	}

	p.versionHead = new(vNode)
	v := new(vNode)
	sk := table.NewSkipList(opts.Topt.MaxLevel, internal.DefaultComparer)
	v.skHead = sk
	p.versionHead.next = v
	v.pre = p.versionHead
	tail := new(vNode)
	v.next = tail
	tail.pre = v
	p.versionTail = tail
	p.pairPool = newPairPool(p.fc)
	//t.diskFunc = diskFunc
	p.gpool = internal.NewGPool(8, internal.Asy)
	return p
}

func (p *Part) StartUpSysmon() {
	go func() {

		for {
			//timer := time.NewTimer(1 * time.Second)
			p.p.tableToBuffer()
			p.p.diskToTable()

			<-time.After(1 * time.Second)
		}

	}()
}

func (part *Part) run() {
	//i := 0
	for {
		select {
		case c := <-part.fc.Db2table:
			//<-part.versionC
			//log.Println("进流控")
			// if part.fc.Ctling.Load() {
			// 	<-part.fc.Signal
			// }
			// dbC := part.fc.GetFromDB
			// if dbC != nil {
			// 	dbC <- c
			// } else {
			// 	<-part.fc.Signal
			// }
			// c1 := <-part.fc.Db2table
			// log.Println("出流控")
			// log.Println(i)
			// log.Println(c.Data[0].Key.Key)
			// i++
			part.Put(c)

		}
	}
}

func (part *Part) Put(c *internal.Chunk) {

	notHotData := make([]*internal.Pair, 0, len(c.Data))
	for i, _ := range c.Data {
		if v, ok := part.MemBuffer.Get(c.Data[i].Key); ok {
			if _, ok := part.updateCache(c.Data[i], v); ok {
				continue
			}

		}
		notHotData = append(notHotData, c.Data[i])
	}
	nc := &internal.Chunk{Data: notHotData}
	accept := part.MemTable.Put(nc)
	//log.Printf("到table,accept:%t\r\n", accept)
	if !accept {
		<-part.versionC
		curr := part.currentVersion()
		part.currentVersionToImmuTable()
		part.versionNum.Add(1)

		//sl.fc.Notify <- fc.Msg{Typ: uint8(2), Pos: uint8(sl.versionNum.Load()), Code: uint8(1)}
		//log.Printf("put通过notify\r\n")
		part.MemTable.Init()

		//sl.diskNotify <- diskN{vNodeAddr: curr, extra: extra}
		//log.Printf("kv/skiplist2.go--run():写盘阶段，Table%d数据%d触发了写盘\r\n", sl.index, newJoin.Data[0].Key.Key)

		//sl.disk(curr)
		//log.Println("table落盘完成")
		//fun := part.Storage.DiskTable
		part.gpool.AsyPut(func() {
			from, to := part.interval.GetData()
			f, t := internal.NewInternalKey(from, 0, 0), internal.NewInternalKey(to, math.MaxUint64, 0)
			_, diskPair := curr.skHead.Range(f, t)

			tableChunk := &internal.Chunk{Index: part.versionSeq, Data: diskPair, Smallest: f, Largest: t, Cmp: internal.DefaultComparer}
			part.versionSeq++
			tableChunk.Callback = func() {
				part.diskComplete(curr)
			}
			part.Storage.DiskTable(tableChunk)
		})
		part.MemTable.Put(nc)
	}
	c = nil

}

func (part *Part) Get(ikey *internal.InternalKey) ([]byte, bool) {

	//k, _ := utils.KeyToHash(key)
	//ikey := internal.NewInternalKey(k, 0, 0)

	var (
		value []byte
		ok    bool
		v     *internal.Pair = nil
	)
	if v, ok = part.MemBuffer.Get(ikey); ok {
		//log.Println(s + "：从cache查到")
		//db.lfuBuffer.Push(ikey.Key)
		return v.Value, ok
	}
	part.versionMutex.RLock()
	defer part.versionMutex.RUnlock()
	ptr := part.versionHead
	for {
		node := ptr.next
		if node == part.versionTail {
			break
		}
		value, hit := node.skHead.Get(ikey)

		if hit {

			return value, true
		}
		ptr = node

	}
	//log.Printf("table没找到%d", ikey.Key)
	if value, ok, _ = part.Storage.Get(ikey); ok {

		//db.lfuBuffer.Push(ikey.Key)
		return value, ok
	}

	return []byte(""), false

}

func (part *Part) updateCache(new *internal.Pair, targetAddr *internal.Pair) (any, bool) {
	newSeq := new.Key.SeqNum
	oldSeq := targetAddr.Key.SeqNum
	if newSeq < oldSeq {
		return nil, false
	}
	old := targetAddr.Value
	targetAddr.Value = new.Value
	targetAddr.Key.SeqNum = new.Key.Key
	targetAddr.Key.Kind = new.Key.Kind
	targetAddr.Key.ValueLen = new.Key.ValueLen
	return old, true
}

func (p *Part) currentVersion() *vNode {
	p.versionMutex.RLock()
	defer p.versionMutex.RUnlock()
	return p.versionHead.next
}

func (p *Part) currentVersionToImmuTable() {
	v := new(vNode)
	sk := p.MemTable.NewSkipList(p.opts.Topt.MaxLevel, internal.DefaultComparer)
	v.skHead = sk
	p.versionMutex.Lock()
	defer p.versionMutex.Unlock()
	current := p.versionHead.next
	p.versionHead.next = v
	current.pre = v
	v.next = current
	v.pre = p.versionHead

}

func (p *Part) deleteVersionNode(vNodeAddr *vNode) {
	p.versionMutex.Lock()
	defer p.versionMutex.Unlock()
	ptr := vNodeAddr.pre
	next := vNodeAddr.next
	ptr.next = next
	next.pre = ptr
	vNodeAddr.pre = nil
	vNodeAddr.next = nil

}
func (sl *Part) diskComplete(node *vNode) {
	sl.deleteVersionNode(node)
	sl.versionNum.Add(-1)
	sl.versionC <- struct{}{}
	//log.Printf("table immuTable落盘回调，version数目是:%d", sl.versionNum.Load())
	//sl.fc.StatisticPos.Store(sl.versionNum.Load())
	//sl.fc.Notify <- fc.Msg{Typ: uint8(1), Pos: uint8(sl.versionNum.Load()), Code: uint8(1)}
	//log.Println("完成后发fc通知成功")
}
