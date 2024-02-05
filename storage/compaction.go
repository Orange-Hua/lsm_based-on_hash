package storage

import (
	"container/heap"
	"container/list"
	"errors"
	"kv/internal"
	"kv/iterator"
	"kv/storage/record"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

const (
	// l0CompactionTrigger is the number of files at which level-0 compaction
	// starts.
	l0CompactionTrigger = 4

	// l0SlowdownWritesTrigger is the soft limit on number of level-0 files.
	// We slow down writes at this point.
	l0SlowdownWritesTrigger = 8

	// l0StopWritesTrigger is the maximum number of level-0 files. We stop
	// writes at this point.
	l0StopWritesTrigger = 12

	// minTableCacheSize is the minimum size of the table cache.
	minTableCacheSize = 64

	// numNonTableCacheFiles is an approximation for the number of MaxOpenFiles
	// that we don't use for table caches.
	numNonTableCacheFiles = 10

	compactSSTableSizeThreshold = 1e9
)

type CompactionManager interface {
	pickCompaction(vs *VersionSet, compactLevel, compactPoint int) (*Compaction, error)
	getSubInterval() *internal.Intervals
	modifyStatus(seq uint64, s status) error
	startCompaction(c *Compaction) (uint64, bool)
	getStatus(seq uint64) (status, bool)
	getSeq(fileNum uint64) uint64
	shouldCompact(size uint64, level int) bool
	logVersionEdit(int, *VersionEdit)
	isFileInCompacting(fileNum uint64) bool
}
type StorageManager interface {
	GetFileSystem() internal.FileSystem
	GetComparer() internal.SeparatorByteComparer
	GetNextFileNumAndMarkUsed() uint64
	GetDir() string
	GetPrevLogNumber() uint64
	GetNextFileNumber() uint64
	GetLogNumber() uint64
	GetLastSequence() uint64
	GetTableCache() *tableCache
	SetPrevLogNumber(uint64)
	SetLogNumber(uint64)
	CreateManifestFromCurrentVersion(vs *VersionSet) error
	GetManifestWriter() *record.Writer
}

type status int

const (
	running = iota

	havingErr
	ending
	waitForCompact
	succs
	notexist
)

type Compaction struct {
	compactLevel int
	compactPoint int
	inputs       [2][]fileMetadata
	version      *Version
	cm           CompactionManager
	seq          uint64
	//newFiles     []newFileEntry
	ve *VersionEdit
	//compactPools            chan []int
	callback                func(*VersionEdit)
	_0levelCompactionSize   int
	_0levelCompactionLength int
	_1levelCompactionSize   int
	completeC               chan *Compaction
}

// func newCompaction( fm StorageManager, _0levelCompactionSize, _1levelCompactionSize int) *Compaction {

// 	return &Compaction{ve: ve, fm: fm, _0levelCompactionSize: _0levelCompactionSize, _1levelCompactionSize: _1levelCompactionSize}
// }

type dir int

const (
	dirReleased dir = iota - 1
	dirSOI
	dirEOI
	dirBackward
	dirForward
)

type mergedIterator struct {
	cmp     internal.ByteComparer
	iters   []iterator.Iterator
	keys    [][]byte
	index   int
	indexs  []int
	err     error //不同操作，一个出现错误如first，全部操作如next，prev都能及时发现
	dir     dir
	reverse bool
	soi     bool
	eoi     bool
}

type indexHeap mergedIterator

func (m *mergedIterator) First() bool {
	if m.err != nil || m.dir == dirReleased {
		return false
	}
	h := m.indexHeap()
	h.Reset(false)
	for i, iter := range m.iters {
		if iter.Next() {
			m.keys[i] = iter.Key()
			h.Push(i)
		} else {
			m.keys[i] = nil
		}

	}
	heap.Init(h)
	m.dir = dirSOI
	return m.next()
}

func (m *mergedIterator) next() bool {
	h := m.indexHeap()
	if h.Len() == 0 {
		m.dir = dirEOI
		return false
	}
	m.index = heap.Pop(h).(int)
	m.dir = dirForward
	return true
}

func (m *mergedIterator) Next() bool {
	if m.err != nil || m.dir == dirEOI {
		return false
	}
	if m.soi == true {
		m.soi = false
		return true
	}
	i := m.index
	iter := m.iters[i]
	h := m.indexHeap()

	if iter.Next() {
		m.keys[i] = iter.Key()
		heap.Push(h, i)

	} else {
		m.keys[i] = nil
	}

	return m.next()
}
func (m *mergedIterator) Key() []byte {
	return m.iters[m.index].Key()
}

func (m *mergedIterator) Value() []byte {
	return m.iters[m.index].Value()
}
func (m *mergedIterator) Close() error {

	for _, iter := range m.iters {
		if err := iter.Close(); err != nil {
			if m.err == nil {
				m.err = err
			}
		}
	}
	return m.err
}
func (m *mergedIterator) indexHeap() *indexHeap {
	return (*indexHeap)(m)
}

func newMergedIterator(iters []iterator.Iterator, cmp internal.ByteComparer) iterator.Iterator {
	m := mergedIterator{}
	m.cmp = cmp
	m.iters = iters
	m.indexs = make([]int, 0, len(iters))
	m.keys = make([][]byte, len(iters))
	m.First()
	m.soi = !m.eoi
	return &m
}

func (c *indexHeap) Push(i any) {
	p := i.(int)

	c.indexs = append(c.indexs, p)

}

func (c *indexHeap) Pop() any {
	item := c.indexs[len(c.indexs)-1]
	c.indexs = c.indexs[:len(c.indexs)-1]
	return item
}

func (c *indexHeap) Reset(reverse bool) {
	c.reverse = reverse
	c.indexs = c.indexs[:0]
}
func (c *indexHeap) Less(i, j int) bool {
	return c.cmp.CompareKey(c.keys[c.indexs[i]], c.keys[c.indexs[j]]) < 0
}

func (c *indexHeap) Len() int {
	return len(c.indexs)
}
func (c *indexHeap) Swap(i, j int) {
	//c.keys[i], c.keys[j] = c.keys[j], c.keys[i]
	c.indexs[i], c.indexs[j] = c.indexs[j], c.indexs[i]
}

func getKeyKind(key []byte) int8 {
	return int8(key[25])
}
func (cm *CompactManager) shouldCompact2(level, subPart int) bool {
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

type CompactStatus struct {
	sta     status
	compact *Compaction
}

type CompactManager struct {
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
	listmutex         sync.Mutex
	fileStatusMutex   sync.Mutex
	maxComapctNum     int
	gpool             internal.GPool
	compacts          chan []int
	zcompacts         chan []int
	vem               *versionEditManager
	ZeroLevlIsRunning atomic.Int32
	completeCompacts  chan *Compaction
}

func NewCompactManager(vs *VersionSet, belong StorageManager, subInterval *internal.Intervals, vem *versionEditManager, concurrComapctNum, oneLevelCompactSize int) *CompactManager {

	cm := CompactManager{}
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
	cm.vem = vem
	return &cm
}

func (c *Compaction) setUpOtherInputs() ([]fileMetadata, error) {

	partNum := c.cm.getSubInterval().GetIntervalNum()
	var fms []fileMetadata
	if c.compactLevel == 0 {
		for i := 0; i < partNum; i++ {
			fm, _, err := c.getNotInCompactingFile(c.compactLevel+1, i)
			if err != nil {
				return nil, err
			}
			if len(fm) == 0 {
				smallest, largest := c.cm.getSubInterval().GetData(i)
				fm = append(fm, fileMetadata{
					fileNum:  0,
					size:     0,
					smallest: internal.InternalKeyToByteArray(internal.NewInternalKey(smallest, 0, 0)),
					largest:  internal.InternalKeyToByteArray(internal.NewInternalKey(largest, 0, 0)),
				})
			}
			fms = append(fms, fm...)

		}

	} else {
		fm, _, err := c.getNotInCompactingFile(c.compactLevel+1, c.compactPoint)
		if err != nil {
			return nil, err
		}
		if len(fm) != 0 {
			fms = append(fms, fm...)
		}

	}
	return fms, nil
}

func (c *Compaction) grow(currSize uint64) {

	startLevel := c.compactLevel + 1
	for i := startLevel; i < len(c.version.files)-1; i++ {

		file, havRunning, err := c.getNotInCompactingFile(i, c.compactPoint)
		if len(file) == 0 || err != nil || havRunning || !c.cm.shouldCompact(totalSize(file)+currSize, c.compactLevel) {
			return
		}

		c.compactLevel = i
		c.inputs[0] = append(c.inputs[0], file...)
		currSize += totalSize(file)
	}

}

/*
//除去正在压缩的table,
*/
func (c *Compaction) getNotInCompactingFile(compactLevel, compactPoint int) (fms []fileMetadata, havRunning bool, err error) {
	if compactLevel >= len(c.version.files) {
		return nil, false, errors.New("index out of version Array")
	}

	files := c.version.files[compactLevel][compactPoint]
	l := 0
	for {
		if l == len(files) {
			break
		}
		if c.cm.isFileInCompacting(files[l].fileNum) {
			l = l + 1
		} else {
			break
		}
	}
	if l != 0 {
		havRunning = true
	}
	return files[l:], havRunning, nil

}

func (cm *CompactManager) shouldCompact(size uint64, level int) bool {
	return size/uint64(cm.maxBytes[level]) > 1
}

func (s *Storage) writeMemTable(chunk *internal.Chunk) (*VersionEdit, error) {
	table, err, fileNum := CreateSSTableByFileNum(s)
	if err != nil {
		log.Fatalf("kv/storage/compaction.go compactMemTable 创建文件失败")
		return nil, err
	}

	//s.markFileNumUsed(fileNum)

	smallest, largest := internal.InternalKeyToByteArray(chunk.Smallest), internal.InternalKeyToByteArray(chunk.Largest)
	w := NewWriter(table, nil)

	data := chunk.Data
	for i := range data {
		key := data[i].Key
		value := data[i].Value

		//log.Printf("skiplist向磁盘写入%d\r\n", key.Key)
		bkey := internal.InternalKeyToByteArray(key)
		//bvalue := utils.AnyToByteArray(value)
		err := w.Set(bkey, value)
		if err != nil {
			log.Fatalf("kv/storage/compaction.go compactMemTable写入数据失败")
			return nil, err
		}
	}
	//log.Printf("skiplist向磁盘写入完成----------------\r\n")
	if err := w.Close(); err != nil {
		return nil, err
	}
	table.Close()
	table, err = OpenSSTableByFileNum(fileNum, s)
	if err != nil {
		return nil, err
	}
	defer table.Close()
	fileInfo, err := table.Stat()
	if err != nil {
		log.Fatalf("kv storage/compaction.go compactMemTable获取文件尺寸失败")
		return nil, err
	}

	ve := VersionEdit{

		partNum:   s.subInterval.GetIntervalNum(),
		logNumber: s.logNumber,
		newFiles: []newFileEntry{
			{
				subPart: 0,
				level:   0,
				meta: fileMetadata{
					fileNum: fileNum, size: uint64(fileInfo.Size()), smallest: smallest, largest: largest,
				},
			},
		},
	}
	return &ve, nil

}

func CreateSSTableByFileNum(fm StorageManager) (internal.File, error, uint64) {
	fileNum := fm.GetNextFileNumAndMarkUsed()
	dir := fm.GetDir()
	fs := fm.GetFileSystem()
	fileName := dbFilename(dir, fileTypeTable, fileNum)
	file, err := fs.Create(fileName)

	return file, err, fileNum

}

func OpenSSTableByFileNum(fileNum uint64, fm StorageManager) (internal.File, error) {
	dir := fm.GetDir()
	fs := fm.GetFileSystem()
	fileName := dbFilename(dir, fileTypeTable, fileNum)
	return fs.Open(fileName)
}

func DeleteSStable(fileNum uint64, fm StorageManager) error {
	dir := fm.GetDir()
	fs := fm.GetFileSystem()
	fileName := dbFilename(dir, fileTypeTable, fileNum)
	if err := fs.Remove(fileName); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err

	}
	return nil
}
