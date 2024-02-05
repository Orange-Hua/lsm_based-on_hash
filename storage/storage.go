package storage

import (
	"encoding/binary"
	"fmt"
	"kv/config"
	"kv/fc"
	"kv/internal"
	"kv/storage/record"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Storage struct {
	dir string

	//diskStart    chan int32
	interval *internal.Interval
	diskNum  int32
	//diskComplete chan struct{}
	dpolicyC chan *internal.Chunk
	// stopC     chan struct{}
	// compactC  chan struct{}
	// curSTable io.Writer
	upKeys []internal.InternalKey
	//lru       SegmentLru
	cmp         internal.SeparatorByteComparer
	defaultRopt *ReadOpt
	storageOpt  *config.StorageOpt
	// ves         chan *VersionEdit
	log *record.Writer
	// Storages       []*Storage
	lru *diskLru
	fc  *fc.FlowCtl
	vs  *VersionSet
	Vs  *VersionSet
	fs  internal.FileSystem
	cm  *CmAsy
	Cm  *CmAsy
	tc  *tableCache
	TC  *tableCache

	logNumber          uint64
	prevLogNumber      uint64
	nextFileNumber     atomic.Uint64
	lastSequence       uint64
	manifestFileNumber uint64
	manifestLock       sync.Mutex
	manifestFile       internal.File
	manifest           *record.Writer

	//diskStart    chan int32Put(
	downC       chan *internal.Chunk
	upC         chan *internal.Chunk
	subInterval *internal.Intervals
	gpool       *internal.GPool
	readBuffer  chan internal.UniqeKey
	vem         vem
	wlog        *record.Writer

	diskComplete   chan struct{}
	nextVersionSeq int
	queue          *veQueue
}

func (s *Storage) GetFileSystem() internal.FileSystem {
	return s.fs
}
func (s *Storage) GetComparer() internal.SeparatorByteComparer {
	return s.cmp
}

func (s *Storage) GetNextFileNumber() uint64 {
	return s.nextFileNumber.Load()
}
func (s *Storage) SetFileNumUsed(fileNum uint64) {
	s.markFileNumUsed(fileNum)
}
func (s *Storage) GetDir() string {
	return s.dir
}

func (s *Storage) GetLogNumber() uint64 {
	return s.logNumber
}

func (s *Storage) GetPrevLogNumber() uint64 {
	return s.prevLogNumber
}

func (s *Storage) SetLogNumber(l uint64) {
	s.logNumber = l
}

func (s *Storage) SetPrevLogNumber(p uint64) {
	s.prevLogNumber = p
}
func (s *Storage) GetNextFileNumAndMarkUsed() uint64 {
	num := s.nextFileNumber.Add(1)
	return num - 1
}
func (s *Storage) GetLastSequence() uint64 {
	return s.lastSequence
}

func (s *Storage) markFileNumUsed(fileNum uint64) {
	oldValue := s.nextFileNumber.Load()
	for {
		if oldValue < fileNum {
			if s.nextFileNumber.CompareAndSwap(oldValue, fileNum+1) {
				break
			}
			oldValue = s.nextFileNumber.Load()

		} else {
			break
		}
	}

}

func (s *Storage) nextFileNum() uint64 {
	return s.nextFileNumber.Add(1)

}
func NewStorage(
	fc *fc.FlowCtl,
	interval *internal.Interval,
	dir string,
	sopt *config.StorageOpt) *Storage {
	// s := Storage{}

	s := Storage{}
	s.dir = dir
	s.defaultRopt = &ReadOpt{
		verifyChecksums: sopt.VerifyChecksums,
		dataCmp:         internal.DefaultByteComparer,
		kcmp:            internal.BytesComparer,
	}

	if sopt.UseFilterPolicy {
		s.defaultRopt.fp = nil
	}
	s.storageOpt = sopt

	s.diskComplete = make(chan struct{}, sopt.MaxBufferNum)
	s.queue = newVeQueue(sopt.MaxBufferNum)
	s.interval = interval
	s.fc = fc
	//s.diskComplete = diskCompleteC
	s.dpolicyC = make(chan *internal.Chunk, 8)
	s.vs = &VersionSet{}
	s.Vs = s.vs
	s.fs = newFileSystem()
	s.tc = newTableCache(s.dir, s.fs, s.defaultRopt, s.storageOpt.TableCacheSize)
	s.TC = s.tc
	s.readBuffer = make(chan internal.UniqeKey, 256)
	//s.lru = newDiskLru(s.readBuffer, 20000)
	s.cmp = internal.DefaultByteComparer

	s.init(s.dir)

	start, end := s.interval.GetData()
	subStorageNum := s.storageOpt.SubPartNum
	if subStorageNum == 1 {

		s.subInterval = &internal.Intervals{Data: [][2]uint64{{start, end}}}
	} else {
		dis := uint64((end - start) / uint64(subStorageNum))
		interval := make([][2]uint64, subStorageNum)
		for i := 0; i < subStorageNum; i++ {
			interval[i][0], interval[i][1] = start, start+dis-1
			start += dis
		}
		interval[subStorageNum-1][1] = end
		s.subInterval = &internal.Intervals{Data: interval}
	}
	s.cm = NewCmAsy(s.vs, &s, s.subInterval, sopt.ConcurCompactionNum, sopt.OnelevelCompactionSize, sopt.ZeroCompactionTrigger, sopt.ZeroMaxWritesTrigger, sopt.MaxBufferNum, fc)
	s.Run()
	//NewSynVEManagerAndCompactionManager(&s, sopt, &s)
	//NewAsyVEManagerAndCompactionManager(&s, sopt, &s)
	// s.Cm = s.cm
	// go s.cm.Compact()
	// }

	// go func() {
	// 	for {
	// 		timer := time.NewTimer(20 * time.Second)
	// 		select {
	// 		case <-timer.C:
	// 			s.vem.deleteVersion()
	// 		}
	// 	}
	// }()
	return &s
}

// func NewSynVEManagerAndCompactionManager(s *Storage, sopt *config.StorageOpt, fm StorageManager) {
// 	s.vem = newVemSyn(s.vs, sopt.ZeroLevelCompactionLength, sopt.MaxBufferNum, s)
// 	s.cm = NewCmSyn(s.vs, s, s.subInterval, s.vem, s.storageOpt.ConcurCompactionNum, s.storageOpt.OnelevelCompactionSize)
// 	s.vem.(*vemSyn).CompactFn = s.cm.(*CmSyn).Compact
// 	s.Cm = s.cm

// }

// func NewAsyVEManagerAndCompactionManager(s *Storage, sopt *config.StorageOpt, fm StorageManager) {
// 	zcompact := make(chan []int, sopt.ZeroLevelCompactionLength)
// 	s.vem = newvemAsy(s.vs, sopt.ZeroLevelCompactionLength, sopt.MaxBufferNum, s, zcompact)
// 	s.cm = NewCmAsy(s.vs, s, s.subInterval, s.vem, s.storageOpt.ConcurCompactionNum, s.storageOpt.OnelevelCompactionSize, zcompact)
// 	go s.cm.(*CmAsy).Compact()
// }

func (p *Storage) Get(key *internal.InternalKey) ([]byte, bool, error) {
	subIndex, ok := p.subInterval.GetIndex(key.Key)
	if !ok {
		return nil, false, nil
	}
	cur := p.vs.refCurrentVersion()

	if cur == nil {
		return nil, false, nil
	}
	defer cur.delRef()
	return cur.get(subIndex, internal.InternalKeyToByteArray(key), p.tc, p.defaultRopt)
}

func (s *Storage) WriteMemTable(chunk *internal.Chunk, callback func()) {
	ve, err := s.writeMemTable(chunk)
	if err != nil {
		log.Fatalf("kv/storage storage.go Write0Level mmtable写入sstable失败: %v", err)
		return
	}
	//s.ves <- ve
	s.vem.WritePut(ve)
}

// func (s *Storage) writeVe() {
// 	n := 0
// 	var ve = VersionEdit{}
// 	for {
// 		v := <-s.ves
// 		ve.newFiles = append(ve.newFiles, v.newFiles...)
// 		ve.logNumber = v.logNumber
// 		n++
// 		if n == int(s.diskNum) {
// 			s.logAndApply(&ve)
// 			ve = VersionEdit{}
// 			n = 0
// 		}
// 	}
// }

func (s *Storage) BackgroundCompaction() {

}

func (s *Storage) T3() {

}

// 4821 2212  7405  4807
// 18420017362026017401
func WithDefaultVersion() *Version {
	v := Version{}

	return &v
}

func (s *Storage) ChooseKeyToUp(c chan *internal.Chunk) {
	go func() {
		record := make(map[internal.UniqeKey]int, 16)
		dup := make([]internal.UniqeKey, 0, 4)
		first := false
		for chunk := range c {
			if !first {
				first = true
			}
			log.Println("kv/storage/storage.go--ChooseKeyToup:接收到晋升数据名单")

			for i := range chunk.Data {
				if _, ok := record[chunk.Data[i].Key.UniqeKey]; ok {
					dup = append(dup, chunk.Data[i].Key.UniqeKey)

					continue
				}
				record[chunk.Data[i].Key.UniqeKey] = 1

			}

			s.dpolicyC <- chunk

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

func (s *Storage) WriteLog(key *internal.InternalKey, value []byte) error {

	log := dbFilename(s.dir, fileTypeLog, s.nextFileNum())

	flog, err := s.GetFileSystem().Open(log)
	defer flog.Close()
	if err != nil {
		return err
	}
	s.wlog = record.NewWriter(flog)
	w, err := s.wlog.Next()
	if err != nil {
		return err
	}
	var buffer []byte
	buffer = append(buffer, internal.InternalKeyToByteArray(key)...)
	binary.PutUvarint(buffer, key.ValueLen)
	buffer = append(buffer, value...)
	w.Write(buffer)
	s.wlog.Flush()
	s.wlog.Close()
	return nil
}
func (s *Storage) Compact() error {

	return nil
}

func (p *Storage) clean() {
loop:
	for {
		timer := time.NewTimer(time.Second * 10)
		<-timer.C
		curVersion := p.vs.currentVersion()
		if curVersion == nil {
			continue
		}
		slow := curVersion
		fast := curVersion.prev
		for {
			if fast == nil {
				break
			}
			refCount := fast.ref.Load()
			if refCount == 0 {
				log.Println("执行删除")
				for i := 0; i < len(fast.next.deletes); i++ {
					if err := DeleteSStable(fast.next.deletes[i], p); err != nil {

						continue loop
					}
				}
			} else {
				fast.next = slow
				slow.prev = fast
				slow = slow.prev
			}
			fast = fast.prev

		}
	}
}

func (s *Storage) DiskTable(c *internal.Chunk) {
	//log.Printf("落盘%d", c.Index)
	ve, err := s.writeMemTable(c)
	if err != nil {
		return
	}
	s.queue.Push(&veWithCallback{ve: ve, callback: []func(){c.Callback}, index: c.Index})
	s.diskComplete <- struct{}{}
}

func (p *Storage) Run() error {

	//loop:
	go func() {
		go p.cm.Compact()
		//go p.clean()
		for {
			select {
			// case <-p.stopC:
			// 	return nil
			// case n := <-s.diskStart:
			// 	log.Printf("kv/storage/storage.go/run():接到table的写盘开始请求,每个sstable被切分成%d份\r\n", n)
			// 	s.diskNum = n
			// 	s.diskStart <- 1
			// case c := <-p.fc.GetFromTable:

			// 	//log.Printf("kv/storage/storage.go/run():接到table的写盘数据，最小值是%d，最大值是%d\r\n", c.Smallest.Key, c.Largest.Key)
			// 	log.Printf("落盘%d", c.Index)
			// 	p.gpool.AsyPut(func() {
			// 		ve, err := p.writeMemTable(c)
			// 		if err != nil {
			// 			return
			// 		}
			// 		p.queue.Push(&veWithCallback{ve: ve, callback: []func(){c.Callback}, index: c.Index})
			// 		p.diskComplete <- struct{}{}
			// 	})

			//p.Write0Level(c)

			//log.Printf("kv/storage/storage.go/run():接到table的写盘数据，落完盘写入队列\r\n")
			//c.Sign <- c.Extra
			//p.cm.Compact(0, 0)
			//p.cm.tablecompacts <- struct{}{}

			//p.cm.Compact(0, 0)
			//s.BackgroundCompaction()
			// for c := range s.downC {
			// 	log.Printf("kv/storage/storage.go/run():接到table的写盘数据，最小值是%d，最大值是%d\r\n", c.Smallest.Key, c.Largest.Key)
			// 	s.Write0Level(c)
			// 	n++
			// }

			case upchunk := <-p.dpolicyC:
				//log.Println("kv/storage/storage.go--Run()：接收到晋升数据名单，准备上传数据")
				var ps = make([]*internal.Pair, 0)
				for i := range upchunk.Data {

					res, ok, err := p.Get(upchunk.Data[i].Key)
					if !ok || err != nil {
						continue
					}
					ps = append(ps, &internal.Pair{Key: upchunk.Data[i].Key, Value: res})
				}
				//p.upC <- &internal.Chunk{Data: ps}
				//log.Printf("kv/storage/storage.go--Run()：上传数据完成，一共上传%d个数据\r\n", len(ps))
			case <-p.diskComplete:
				item := p.queue.Pop()
				if item != nil {
					//log.Printf("落盘%d完成", item.(*veWithCallback).index)
					p.cm.immuTableC <- item.(*veWithCallback)
				}

			}
		}
	}()
	return nil
}

func (s *Storage) GetbyUkey(ukey *internal.UniqeKey) {

}
func (s *Storage) GetManifestWriter() *record.Writer {
	return s.manifest
}
func (s *Storage) CreateManifestFromCurrentVersion(vs *VersionSet) (err error) {
	var (
		filename     = dbFilename(s.dir, fileTypeManifest, s.manifestFileNumber)
		manifestFile internal.File
		manifest     *record.Writer
	)
	defer func() {
		if manifest != nil {
			manifest.Close()
		}
		if manifestFile != nil {
			manifestFile.Close()
		}
		if err != nil {
			s.fs.Remove(filename)
		}
	}()
	manifestFile, err = s.fs.Create(filename)
	if err != nil {
		return err
	}
	s.markFileNumUsed(s.manifestFileNumber)
	manifest = record.NewWriter(manifestFile)

	snapshot := VersionEdit{
		comparatorName: s.cmp.Name(),
	}
	// TODO: save compaction pointers.
	curVersion := vs.currentVersion()
	if curVersion != nil {
		for level, parts := range vs.currentVersion().files {
			for p, part := range parts {
				for _, meta := range part {
					snapshot.newFiles = append(snapshot.newFiles, newFileEntry{
						subPart: p,
						level:   level,
						meta:    meta,
					})
				}

			}
		}
	}

	w, err1 := manifest.Next()
	if err1 != nil {
		return err1
	}
	err1 = snapshot.encode(w)
	if err1 != nil {
		return err1
	}
	if err := setCurrentFile(s.manifestFileNumber, s.dir, s.fs); err != nil {
		return err
	}
	s.manifest, manifest = manifest, nil
	s.manifestFile, manifestFile = manifestFile, nil
	return nil
}

func (s *Storage) logAndApply(ve *VersionEdit) error {
	if ve.logNumber != 0 {
		if ve.logNumber < s.GetLogNumber() || s.GetNextFileNumber() <= ve.logNumber {
			panic(fmt.Sprintf("leveldb: inconsistent versionEdit logNumber %d", ve.logNumber))
		}
	}
	ve.nextFileNumber = s.GetNextFileNumber()
	ve.lastSequence = s.GetLastSequence()

	var bve bulkVersionEdit
	bve.accumulate(ve)
	newVersion, err := bve.apply(s.vs.currentVersion(), s.GetComparer())
	if err != nil {
		return err
	}
	// if len(newVersion.files[0][0]) >= s.belong.storageOpt.ZeroLevelCompactionLength && s.downC != nil {
	// 	s.downC1 = s.downC
	// 	s.downC = nil
	// }
	// if len(newVersion.files[0][0]) == 0 && s.downC == nil {
	// 	s.downC = s.downC1
	// 	s.downC1 = nil
	// }
	//manifest记录各种versionEdit
	if s.manifest == nil {

		s.CreateManifestFromCurrentVersion(s.vs)
		if err != nil || s.manifest == nil {
			log.Fatalf("kv/storage/storage.go logAndApply创建manifest出错")
			return err
		}

	}
	//将versionEdit写入manifest
	w, err := s.manifest.Next()
	if err != nil {
		return err
	}
	if err := ve.encode(w); err != nil {
		return err
	}
	if err := s.manifest.Flush(); err != nil {
		return err
	}
	// if err := s.manifestFile.Sync(); err != nil {
	// 	return err
	// }

	s.vs.append(newVersion) //一个新版本，用新的log号，也就是合并产生的新version对应的ve的log号
	if ve.logNumber != 0 {
		s.SetLogNumber(ve.logNumber)
	}
	if ve.prelogNumber != 0 {
		s.SetPrevLogNumber(ve.prelogNumber)
	}
	// vv := s.vs.currentVersion()
	// for i := range vv.files[0] {
	// 	log.Printf("新version的0层的文件文件号是%d\r\n", vv.files[0][i].fileNum)
	// }
	return nil
}

func (s *Storage) GetTableCache() *tableCache {
	return s.tc
}
func (s *Storage) GetLowHotItems(num int, sync bool) []internal.UniqeKey {
	//return s.lru.GetFewInactive(num, sync)
	return nil
}

func (s *Storage) GetFewActive(num int, sync bool) []internal.UniqeKey {
	return nil
	//return s.lru.GetFewActive(num, sync)
}
func (s *Storage) GetCurrentCapacity() uint {
	return 0
}

type veQueue struct {
	ves     []*veWithCallback
	nextSeq int
	mutex   sync.Mutex
}

func newVeQueue(capacity int) *veQueue {
	return &veQueue{ves: make([]*veWithCallback, 0, capacity), nextSeq: 0}
}
func (c *veQueue) Push(i any) {
	p := i.(*veWithCallback)
	c.mutex.Lock()

	c.ves = append(c.ves, p)
	c.mutex.Unlock()
}
func (c *veQueue) Front() any {

	return c.ves[len(c.ves)-1]
}
func (c *veQueue) Pop() any {
	c.mutex.Lock()
	if c.Front().(*veWithCallback).index != c.nextSeq {
		c.mutex.Unlock()
		return nil
	}
	item := c.ves[len(c.ves)-1]
	c.ves = c.ves[:len(c.ves)-1]
	c.nextSeq = c.nextSeq + 1
	c.mutex.Unlock()
	return item
}

func (c *veQueue) Reset(reverse bool) {
	//c.reverse = reverse
	c.mutex.Lock()
	c.ves = c.ves[:0]
	c.mutex.Unlock()
}
func (c *veQueue) Less(i, j int) bool {

	return c.ves[i].index < c.ves[j].index
}

func (c *veQueue) Len() int {
	return len(c.ves)
}
func (c *veQueue) Swap(i, j int) {
	//c.keys[i], c.keys[j] = c.keys[j], c.keys[i]
	c.ves[i], c.ves[j] = c.ves[j], c.ves[i]
}
