package storage

import (
	"container/list"
	"errors"
	"kv/fc"
	"kv/internal"
	"kv/iterator"
	"log"

	"math"
	"sync"
	"sync/atomic"
)

/*
version 差分删除+协程定时删除  √
compaction 并发compact函数，写成并发的  √
compact加链表，防止死锁        √
1层合并文件的新建问题  √
skiplist Get的去锁
disk get读空数据问题
*/

// func newCompaction( fm StorageManager, _0levelCompactionSize, _1levelCompactionSize int) *Compaction {

// 	return &Compaction{ve: ve, fm: fm, _0levelCompactionSize: _0levelCompactionSize, _1levelCompactionSize: _1levelCompactionSize}
// }

func (cm *CmAsy) shouldCompact2(level, subPart int) bool {
	cur := cm.vs.currentVersion()
	files := cur.files[level][subPart]
	if len(files) == 0 {
		return false
	}
	for i := range files {
		if _, ok := cm.compactingFiles[files[i].fileNum]; ok {
			continue
		}
		if cm.shouldCompact(files[i].size, level) {
			return true
		}
	}
	return false
}
func (cm *CmAsy) execute(com []int) {
	level := com[0]
	subPart := com[1]
	com = nil

	if !cm.shouldCompact2(level, subPart) {
		return

	}
	c, err := cm.pickCompaction(cm.vs, level, subPart)
	if err != nil || c == nil {
		//log.Printf("kv/storage/comaction.go/Compact():%d,%d,没有需要压缩的对象\r\n", level, subPart)
		return
	}

	c.completeC = cm.completeCompacts
	c.callback = func(ve *VersionEdit) {

		if ve == nil {
			return
		}
		//log.Printf("%d,%d\r\n", c.compactLevel, c.compactPoint)
		c.ve = ve

		// vesion := cm.vs.currentVersion()
		// files := vesion.files
		// for i := 1; i < 4; i++ {
		// 	for j := 0; j < 4; j++ {
		// 		for _, f := range files[i][j] {
		// 			fmt.Printf("执行输出的是：%d,%d,level%d--part%d: 【file:%d】\r\n", level, subPart, i, j, f.fileNum)
		// 		}

		// 	}
		// }

		c.completeC <- c

	}

	seq, ok := cm.startCompaction(c)
	if !ok {
		return
	}
	c.seq = seq
	cm.gpool.AsyPut(func() {
		cm.compact(c)
	})
	//log.Println("压缩开始执行")
}

func (cm *CmAsy) MergeImmuTable(ves []*veWithCallback, addr uintptr, callback func(*veWithCallback, uintptr)) {
	n := len(ves)
	meta := make([]fileMetadata, 0, n)
	for i := 0; i < n; i++ {
		meta = append(meta, ves[i].ve.newFiles[0].meta)
	}

	c := Compaction{cm: cm, compactLevel: -1, compactPoint: 0}
	c.inputs[0] = meta
	c.callback = func(ve *VersionEdit) {
		funcs := make([]func(), 0, n)
		for i := 0; i < n; i++ {
			funcs = append(funcs, ves[i].callback...)
		}
		vewithcallback := &veWithCallback{ve: ve, callback: funcs}
		callback(vewithcallback, addr)
		cm.completeCompacts <- &c
	}
	cm.gpool.AsyPut(func() {
		cm.compact(&c)
	})
}
func (cm *CmAsy) Compact() error {
	for {
		select {
		case vecallback := <-cm.immuTableC:

			cm.vem.WritePut(vecallback)

		case com := <-cm.zcompacts:
			cm.execute(com)
		case com := <-cm.compacts:
			cm.execute(com)
		case cc := <-cm.completeCompacts:

			n := len(cc.ve.newFiles)
			cm.vem.CompactPut(cc.compactLevel, cc.ve)

			cm.modifyStatus(cc.seq, notexist)
			for i := 0; i < n; i++ {
				select {
				case cm.compacts <- []int{cc.ve.newFiles[i].level, cc.ve.newFiles[i].subPart}:
				default:
				}

			}
			//log.Printf("%d,%d压缩执行完毕", cc.compactLevel, cc.compactPoint)

			cc = nil

		}
	}

}

func (cm *CmAsy) compact(c *Compaction) error {

	//log.Printf("%d开了一个线程执行%d,%d\r\n", c.cm.belong.index, c.compactLevel, c.compactPoint)

	inputs := c.inputs
	if c.compactLevel != 0 && len(inputs[1]) == 0 && len(inputs[0]) == 1 { //input[0]只能是1个

		if len(inputs[0]) == 0 {
			return nil
		}

		partIndex, _ := cm.subInterval.GetIndex(internal.ByteArrayToInternalKey(c.inputs[0][0].smallest).Key)
		ve := &VersionEdit{
			partNum: cm.subInterval.GetIntervalNum(),

			deletedFiles: map[deletedFileEntry]bool{
				{
					subPart:     partIndex,
					level:       c.compactLevel,
					fileNum:     c.inputs[0][0].fileNum,
					logicDelete: 1,
				}: true,
			},
			newFiles: []newFileEntry{
				{
					subPart: partIndex,
					level:   c.compactLevel + 1,
					meta: fileMetadata{
						fileNum:  c.inputs[0][0].fileNum,
						smallest: c.inputs[0][0].smallest,
						largest:  c.inputs[0][0].largest,
						size:     c.inputs[0][0].size,
					},
				},
			},
		}
		c.callback(ve)
		return nil
	}

	iters := []iterator.Iterator(nil)
	for i := 0; i < 2; i++ {
		for j := 0; j < len(inputs[i]); j++ {
			if inputs[i][j].fileNum == 0 {
				continue
			}
			iter, err := cm.fm.GetTableCache().find(inputs[i][j].fileNum, nil, nil)
			if err != nil {
				log.Println("kv/storage/compaction.go--merge():合并失败")
				return err
			}
			iters = append(iters, iter)
		}
	}

	m := newMergedIterator(iters, cm.fm.GetComparer())
	defer m.Close()
	num := len(c.inputs[1])
	if num == 0 {
		num = 1
	}
	tws := make([]*Writer, num)
	fms := make([]fileMetadata, num)

	var (
		file    internal.File
		err     error
		fileNum uint64
	)

	for i := 0; i < num && len(inputs[1]) != 0; i++ {
		fms[i] = inputs[1][i]
	}
	if len(inputs[1]) == 0 {
		fms[0] = fileMetadata{
			fileNum:  0,
			smallest: inputs[0][0].smallest,
			largest:  inputs[0][0].largest,
		}
	}
	for i := 0; i < num; i++ {

		file, err, fileNum = CreateSSTableByFileNum(cm.fm)
		if err != nil {
			log.Println("kv/storage/compaction.go--compact():创建新的合并文件失败")
			return err
		}
		fms[i].fileNum = fileNum

		tw := NewWriter(file, nil)
		if err != nil {

			return err
		}
		tws[i] = tw

	}

	var preKey []byte
	//smallest = append(smallest[:0], m.Key()...)

	i := 0
	for m.Next() {
		key := m.Key()
		value := m.Value()

		if cm.fm.GetComparer().CompareUniqeKey(key, fms[i].largest) > 0 {
			b := internal.ByteArrayToInternalKey(key)
			i, _ = cm.subInterval.GetIndex(b.Key)
			//log.Println("更换写文件")

		}
		if cm.fm.GetComparer().CompareUniqeKey(key, preKey) == 0 {
			continue
		}
		if internal.Kind(key) == internal.InternalKeyKindDelete {
			continue
		}

		if err := tws[i].Set(key, value); err != nil {
			cm.modifyStatus(c.seq, havingErr)
			return err
		}
		preKey = append(preKey[:0], key...)

	}
	for i := 0; i < num; i++ {
		tws[i].Close()
	}

	ve := &VersionEdit{
		partNum:      c.cm.getSubInterval().GetIntervalNum(),
		deletedFiles: map[deletedFileEntry]bool{},
		newFiles:     make([]newFileEntry, 0, num),
	}
	for index := 0; index < num; index++ {
		file, err := OpenSSTableByFileNum(fms[index].fileNum, cm.fm)
		if err != nil {
			log.Printf("kv/storage/compaction.go-compact():打开合并文件出错\r\n")
			return err
		}
		fileInfo, err := file.Stat()
		if err != nil {
			log.Printf("kv/storage/compaction.go-compact():获取合并文件信息出错\r\n")
			return err
		}
		partIndex, _ := cm.subInterval.GetIndex(internal.ByteArrayToInternalKey(fms[index].smallest).Key)

		ve.newFiles = append(ve.newFiles, newFileEntry{
			subPart: partIndex,
			level:   c.compactLevel + 1,
			meta: fileMetadata{
				fileNum:  fms[index].fileNum,
				smallest: fms[index].smallest,
				largest:  fms[index].largest,
				size:     uint64(fileInfo.Size()),
			},
		})

	}

	for i := 0; i < 2; i++ {
		for j := 0; j < len(inputs[i]); j++ {
			if inputs[i][j].fileNum == 0 {
				continue
			}
			partIndex, _ := cm.subInterval.GetIndex(internal.ByteArrayToInternalKey(inputs[i][j].smallest).Key)
			ve.deletedFiles[deletedFileEntry{
				subPart: partIndex,
				level:   c.compactLevel + i,
				fileNum: inputs[i][j].fileNum,
			}] = true
		}
	}

	//log.Println("kv/storage/compaction.go--compact()执行完")
	c.callback(ve)
	return nil
}

func (cm *CmAsy) modifyStatus(seq uint64, s status) error {

	if s == notexist {
		v, ok := cm.status[seq]
		if !ok {
			return errors.New("没有该压缩")
		}
		inputs := v.compact.inputs
		for i := 0; i < len(inputs); i++ {
			for _, f := range inputs[i] {
				delete(cm.compactingFiles, f.fileNum)
			}
		}
		delete(cm.status, seq)
		return nil
	}
	cm.status[seq].sta = s
	return nil
}

func (cm *CmAsy) startCompaction(c *Compaction) (uint64, bool) {
	inputs := c.inputs

	for i := 0; i < len(inputs); i++ {
		for _, f := range inputs[i] {
			if _, ok := cm.compactingFiles[f.fileNum]; ok {
				return math.MaxUint64, false
			}
		}
	}
	seq := cm.compactNum.Add(1)
	for i := 0; i < len(inputs); i++ {
		for _, f := range inputs[i] {
			cm.compactingFiles[f.fileNum] = seq
		}
	}

	cm.status[seq] = &CompactStatus{running, c}

	return seq, true
}
func (cm *CmAsy) getStatus(seq uint64) (status, bool) {
	cm.rwmutex.RLock()
	defer cm.rwmutex.RUnlock()
	v, ok := cm.status[seq]
	if !ok {
		return notexist, ok
	}
	return v.sta, ok
}

func (cm *CmAsy) getSeq(fileNum uint64) uint64 {
	return cm.compactingFiles[fileNum]
}

type CmAsy struct {
	status   map[uint64]*CompactStatus
	maxBytes []int //每一层最大字节限制

	subInterval       *internal.Intervals
	fm                StorageManager
	compactingFiles   map[uint64]uint64 //fileNum:seqNum
	fileStatus        map[uint64]status
	vs                *VersionSet
	compactNum        atomic.Uint64
	rwmutex           sync.RWMutex
	tasks             chan *Compaction
	notify            chan struct{}
	list              *list.List
	fc                *fc.FlowCtl
	listmutex         sync.Mutex
	fileStatusMutex   sync.Mutex
	maxComapctNum     int
	gpool             internal.GPool
	compacts          chan []int
	zcompacts         chan []int
	immuTableC        chan *veWithCallback
	vem               *vemAsy
	ZeroLevlIsRunning atomic.Int32
	completeCompacts  chan *Compaction
}

func NewCmAsy(vs *VersionSet, belong StorageManager, subInterval *internal.Intervals, concurrComapctNum, oneLevelCompactSize, zeroCompactionTrigger, zeroMaxWritesTrigger int, maxBufferNum int, fc *fc.FlowCtl) *CmAsy {

	cm := CmAsy{}
	cm.maxComapctNum = concurrComapctNum
	cm.status = make(map[uint64]*CompactStatus)
	cm.compactingFiles = make(map[uint64]uint64)
	cm.vs = vs
	cm.gpool = *internal.NewGPool(cm.maxComapctNum, internal.Asy)
	cm.fm = belong
	cm.subInterval = subInterval
	cm.tasks = make(chan *Compaction, cm.maxComapctNum)
	cm.notify = make(chan struct{}, 1)
	cm.maxBytes = []int{1, oneLevelCompactSize, 10 * oneLevelCompactSize, 10 * 10 * oneLevelCompactSize}
	cm.list = list.New()
	cm.compacts = make(chan []int, 1024)
	cm.zcompacts = make(chan []int, zeroMaxWritesTrigger)
	cm.immuTableC = make(chan *veWithCallback, 2*zeroMaxWritesTrigger)
	cm.completeCompacts = make(chan *Compaction, 1024)
	cm.vem = newvemAsy(cm.vs, zeroCompactionTrigger, zeroMaxWritesTrigger, maxBufferNum, cm.fm, cm.zcompacts, cm.MergeImmuTable, fc)

	return &cm
}
func (cm *CmAsy) pickCompaction(vs *VersionSet, compactLevel, compactPoint int) (*Compaction, error) {

	curVersion := cm.vs.currentVersion() //每层，每分片，每个filemeta
	c := &Compaction{version: curVersion, cm: cm}
	c.inputs[0] = make([]fileMetadata, 0, cm.subInterval.GetIntervalNum())
	c.inputs[1] = make([]fileMetadata, 0, cm.subInterval.GetIntervalNum())
	c.compactLevel = compactLevel
	c.compactPoint = compactPoint

	f, havRunning, _ := c.getNotInCompactingFile(compactLevel, compactPoint)
	if havRunning || len(f) == 0 || !cm.shouldCompact(totalSize(f), compactLevel) {
		return nil, nil
	}
	c.inputs[0] = append(c.inputs[0], f...)
	size := totalSize(c.inputs[0])
	if compactLevel > 0 {
		c.grow(size)
	}
	fms, _ := c.setUpOtherInputs()
	c.inputs[1] = append(c.inputs[1], fms...)
	return c, nil
}
func (cm *CompactManager) getStatus(seq uint64) (status, bool) {
	cm.rwmutex.RLock()
	defer cm.rwmutex.RUnlock()
	v, ok := cm.status[seq]
	if !ok {
		return notexist, ok
	}
	return v.sta, ok
}
func (cm *CmAsy) CompareAndSwapStatus(fileNum uint64, oldStatus, newStatus status) bool {
	cm.fileStatusMutex.Lock()
	defer cm.fileStatusMutex.Unlock()
	if v, ok := cm.fileStatus[fileNum]; ok && v == oldStatus {
		cm.fileStatus[fileNum] = newStatus
		return true
	}
	return false
}

func (cm *CmAsy) shouldCompact(size uint64, level int) bool {
	return size/uint64(cm.maxBytes[level]) > 1
}

func (cm *CmAsy) logVersionEdit(level int, ve *VersionEdit) {
	cm.vem.CompactPut(level, ve)
}
func (cm *CmAsy) isFileInCompacting(fileNum uint64) bool {
	_, ok := cm.compactingFiles[fileNum]
	return ok
}
func (cm *CmAsy) getSubInterval() *internal.Intervals {
	return cm.subInterval
}
