package storage

import (
	"container/list"
	"fmt"
	"kv/fc"
	"kv/storage/record"
	"log"
	"sync/atomic"
	"unsafe"
)

type vem interface {
	CompactPut(level int, ve *VersionEdit)
	WritePut(ve *VersionEdit)
}

type veNode struct {
	ve *veWithCallback
	s  status
}
type velisit struct {
	waitList     *list.List
	callback     func([]*veWithCallback, uintptr, func(*veWithCallback, uintptr)) //compact函数，生成压缩表
	capacity     int
	mergeNodeNum atomic.Int32
}

func newVeList(callback func([]*veWithCallback, uintptr, func(*veWithCallback, uintptr)), capacity int) *velisit {
	vl := &velisit{}
	vl.waitList = list.New()
	callback = callback
	capacity = capacity
	return vl
}

type veWithCallback struct {
	ve       *VersionEdit
	callback []func() //删除memtable
	index    int
}

func (v *velisit) put(ve *veWithCallback) {
	v.waitList.PushBack(&veNode{ve: ve})
	if v.waitList.Len()-int(v.mergeNodeNum.Load()) >= v.capacity {
		v.merge()

	}
}

func (v *velisit) merge() {
	n := v.capacity
	ves := make([]*veWithCallback, 0, n)

	for i := 0; i < n; i++ {
		ptr := v.waitList.Back()
		f := ptr.Value.(*veWithCallback)
		ves = append(ves, f)
		v.waitList.Remove(ptr)
	}
	dummy := &veNode{&veWithCallback{}, running}
	v.mergeNodeNum.Add(1)
	v.waitList.PushFront(dummy)
	v.callback(ves, uintptr(unsafe.Pointer(dummy)), v.applyCompactTable)

}

func (v *velisit) applyCompactTable(ve *veWithCallback, addr uintptr) {
	nodeAddr := (*veNode)(unsafe.Pointer(addr))
	nodeAddr.ve = ve
	nodeAddr.s = succs

}

type vemAsy struct {
	//vem      *versionEditManager
	//waitlist            *velisit
	waitlist              *list.List
	zcompact              chan []int
	vs                    *VersionSet
	zeroCompactionTrigger int
	zeroMaxWritesTrigger  int
	maxBufferNum          int
	fm                    StorageManager
	manifest              *record.Writer
	fc                    *fc.FlowCtl
}

func newvemAsy(vs *VersionSet, zeroCompactionTrigger int,
	zeroMaxWritesTrigger int, maxBufferNum int, fm StorageManager, zcompact chan []int, callback func([]*veWithCallback, uintptr, func(*veWithCallback, uintptr)),
	fc *fc.FlowCtl) *vemAsy {
	vasy := &vemAsy{
		zeroCompactionTrigger: zeroCompactionTrigger,
		zeroMaxWritesTrigger:  zeroMaxWritesTrigger,
		maxBufferNum:          maxBufferNum,
		zcompact:              zcompact,
		//waitlist: newVeList(callback, maxBufferNum),
		waitlist: list.New(),
		vs:       vs,
		//zeroLevelCompactionLength: zeroLevelCompactionLength,
		fm: fm,
		fc: fc,
	}

	return vasy
}

func (v *vemAsy) fetchFewVeToLog() {
	waiter := v.waitlist.Len()
	cur := v.vs.currentVersion()
	availNum := v.zeroMaxWritesTrigger - len(cur.files[0][0])
	signalNum := min(availNum, int(waiter))
	newVe := v.mergeZeroLevelVersionEdit(signalNum)
	if newVe == nil {
		return
	}
	v.logAndApply(newVe.ve)
	for i := range newVe.callback {
		newVe.callback[i]()
	}
	cur = v.vs.currentVersion()
	if len(cur.files[0][0]) > v.zeroCompactionTrigger {
		select {
		case v.zcompact <- []int{0, 0}:
		default:
		}
	}
}

// func (v *vemAsy) fetchFewVeToLog() {
// 	waiter := v.waitlist.waitList.Len()
// 	cur := v.vs.currentVersion()
// 	availNum := v.l0MaxWritesTrigger - len(cur.files[0][0])
// 	signalNum := min(availNum, int(waiter))
// 	newVe := v.mergeZeroLevelVersionEdit(signalNum)
// 	if newVe == nil {
// 		return
// 	}
// 	v.logAndApply(newVe.ve)
// 	for i := range newVe.callback {
// 		newVe.callback[i]()
// 	}
// 	cur = v.vs.currentVersion()
// 	if len(cur.files[0][0]) > v.l0CompactionTrigger {
// 		v.zcompact <- []int{0, 0}
// 	}
// }

func (v *vemAsy) CompactPut(level int, ve *VersionEdit) {

	//v.vem.manifestMutex.Lock()

	v.logAndApply(ve)
	//v.vem.deleteVersion()
	//v.vem.manifestMutex.Unlock()

	if level == 0 {
		//v.vem.writeMutex.Lock()
		waiter := v.waitlist.Len()
		if waiter == 0 {
			cur := v.vs.currentVersion()
			v.fc.StatisticPos.Store(int32(len(cur.files[0][0])) + int32(v.waitlist.Len()))
			return
		}
		v.fetchFewVeToLog()
		cur := v.vs.currentVersion()
		v.fc.StatisticPos.Store(int32(len(cur.files[0][0])) + int32(v.waitlist.Len()))
		//log.Println("已经将部分ve从等待队列中取出，immutable可以留出位置了")
	}
}
func (v *vemAsy) WritePut(ve *veWithCallback) {

	//同步写table线程
	if ve == nil {
		log.Fatal("memtable写盘失败\r\n")
	}
	//v.vem.writeMutex.Lock()
	cur := v.vs.currentVersion()
	if v.waitlist.Len() == 0 && (cur == nil || len(cur.files[0][0]) < v.zeroMaxWritesTrigger) {
		v.logAndApply(ve.ve)
		for i := range ve.callback {
			ve.callback[i]()
		}
		cur := v.vs.currentVersion()
		if len(cur.files[0][0]) > v.zeroCompactionTrigger && len(cur.files[0][0]) < v.zeroMaxWritesTrigger {
			select {
			case v.zcompact <- []int{0, 0}:
			default:
			}

		}
		v.fc.StatisticPos.Store(int32(len(cur.files[0][0])))
		return
	}
	v.waitlist.PushBack(ve)
	v.fc.StatisticPos.Store(int32(len(cur.files[0][0])) + int32(v.waitlist.Len()))
	// 	if v.vem.writeWaitList.Len() > 0 {

	// 		sigNum := min(v.vem.writeWaitList.Len(), v.vem.zeroLevelCompactionLength-len(cur.files[0][0]))
	// 		ve := v.vem.mergeZeroLevelVersionEdit(sigNum)

	// 		//v.vem.manifestMutex.Lock()
	// 		v.vem.logAndApply(ve)
	// 		//v.vem.manifestMutex.Unlock()
	// 		//v.vem.writeMutex.Unlock()
	// 		v.zcompact <- []int{0, 0}
	// 		callback()
	// 		return
	// 	}
	// }
	// v.vem.writeWaitList.PushBack(ve)

	// // if v.vem.writeWaitList.Len() <= v.vem.maxBufferNum {

	// // 	//v.vem.writeMutex.Unlock()
	// // 	callback()
	// // 	return
	// // }
	// //log.Println("写阻塞了")
	// //v.zcompact <- []int{0, 0}
	// //log.Println("写阻塞后发送压缩00")
	// //v.vem.writeMutex.Unlock()
	// //<-v.vem.syn
	// //log.Println("写被唤醒了")
	// if v.vem.writeWaitList.Len() > zeroLevelMaxTables {

	// }
	// callback()
}
func (vem *vemAsy) mergeZeroLevelVersionEdit(signalNum int) *veWithCallback {

	ns := make([]newFileEntry, 0, signalNum)
	funcs := make([]func(), 0, signalNum)
	partNum := -1

	for i := 0; i < signalNum; i++ {
		ptr := vem.waitlist.Front()
		node := ptr.Value.(*veWithCallback)

		if partNum == -1 {
			partNum = node.ve.partNum
		}
		ns = append(ns, node.ve.newFiles...)
		funcs = append(funcs, node.callback...)
		vem.waitlist.Remove(ptr)

	}
	newVe := &veWithCallback{ve: &VersionEdit{newFiles: ns, partNum: partNum}, callback: funcs}
	return newVe
}

// func (vem *vemAsy) mergeZeroLevelVersionEdit(signalNum int) *veWithCallback {

// 	ns := make([]newFileEntry, 0, signalNum)
// 	funcs := make([]func(), 0, signalNum)
// 	partNum := -1
// 	ptr := vem.waitlist.waitList.Front()
// 	realNum := 0
// 	for i := 0; i < signalNum; i++ {

// 		node := ptr.Value.(*veNode)
// 		if node.s == running {
// 			break
// 		}
// 		if partNum == -1 {
// 			partNum = node.ve.ve.partNum
// 		}
// 		ns = append(ns, node.ve.ve.newFiles...)
// 		funcs = append(funcs, node.ve.callback...)
// 		realNum++
// 		ptr = ptr.Next()
// 	}
// 	for i := 0; i < realNum; i++ {
// 		front := vem.waitlist.waitList.Front()
// 		vem.waitlist.waitList.Remove(front)
// 	}
// 	newVe := &veWithCallback{ve: &VersionEdit{newFiles: ns, partNum: partNum}, callback: funcs}
// 	return newVe
// }

func (s *vemAsy) logAndApply(ve *VersionEdit) error {
	if ve.logNumber != 0 {
		if ve.logNumber < s.fm.GetLogNumber() || s.fm.GetNextFileNumber() <= ve.logNumber {
			panic(fmt.Sprintf("leveldb: inconsistent versionEdit logNumber %d", ve.logNumber))
		}
	}
	ve.nextFileNumber = s.fm.GetNextFileNumber()
	ve.lastSequence = s.fm.GetLastSequence()

	var bve bulkVersionEdit
	bve.accumulate(ve)
	newVersion, err := bve.apply(s.vs.currentVersion(), s.fm.GetComparer())
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

		s.fm.CreateManifestFromCurrentVersion(s.vs)
		s.manifest = s.fm.GetManifestWriter()
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
		s.fm.SetLogNumber(ve.logNumber)
	}
	if ve.prelogNumber != 0 {
		s.fm.SetPrevLogNumber(ve.prelogNumber)
	}
	// vv := s.vs.currentVersion()
	// for i := range vv.files[0] {
	// 	log.Printf("新version的0层的文件文件号是%d\r\n", vv.files[0][i].fileNum)
	// }
	return nil
}

func (vem *vemAsy) deleteVersion() {
	curVersion := vem.vs.currentVersion()
	if curVersion == nil {
		return
	}
	slow := curVersion
	fast := curVersion.prev
	for {
		if fast == nil {

			slow.prev = fast
			break
		}
		refCount := fast.ref.Load()
		if refCount == 0 && fast.ref.CompareAndSwap(refCount, -1) {

			for i := 0; i < len(fast.next.deletes); i++ {

				if err := DeleteSStable(fast.next.deletes[i], vem.fm); err != nil {

					//log.Printf("删除文件%d失败%v\r\n", fast.next.deletes[i], err)
					old := fast.next
					fast.next = slow
					slow.prev = fast
					if old != slow {
						slow.deletes = old.deletes

					}

					slow = slow.prev
				}

			}

			//log.Println("执行删除")

		} else {

			old := fast.next
			fast.next = slow
			slow.prev = fast
			if old != slow {
				slow.deletes = old.deletes

			}

			slow = slow.prev
		}
		fast = fast.prev

	}
}
func min(x int, y int) int {

	if x <= y {
		return x
	}
	if x > y {
		return y
	}
	return -1
}
