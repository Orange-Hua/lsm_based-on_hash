package internal

import (
	"unsafe"
)

var LRUACTIVENUM = 32

type Lru interface {
	Put(UniqeKey)
	Victim() UniqeKey
	GetVictim() UniqeKey
	Size() int
	GetFewActive(int) []UniqeKey
	GetFewInactive(int) []UniqeKey
}

type SegmentLru interface {
	Lru
	GetActiveSize() int
	GetInactiveSize() int
	RemoveNodeByKey(key UniqeKey)
}

type singleLru struct {
	head       *sNode
	tail       *sNode
	bufferKeys map[UniqeKey]*sNode
	size       int
}

type sNode struct {
	key  UniqeKey
	next *sNode
	pre  *sNode
}

func (s *singleLru) Put(key UniqeKey) {
	pos := &sNode{key: key}

	old := s.head.next

	old.pre = pos
	pos.next = old
	pos.pre = s.head
	s.head.next = pos
	s.bufferKeys[key] = pos
	s.size++

}

func (s *singleLru) remove(key UniqeKey) *sNode {
	snode := s.bufferKeys[key]
	next := snode.next
	pre := snode.pre
	pre.next = next
	next.pre = pre
	snode.next = nil
	snode.pre = nil
	delete(s.bufferKeys, key)
	return snode
}

func (s *singleLru) Victim() UniqeKey {

	old := s.tail.pre
	pre := old.pre
	pre.next = s.tail
	s.tail.pre = pre
	old.pre = nil
	old.next = nil
	s.size--
	delete(s.bufferKeys, old.key)
	return old.key
}

func (s *singleLru) get(key UniqeKey) (*sNode, bool) {
	v, ok := s.bufferKeys[key]
	return v, ok

}

func (s *singleLru) GetVictim() UniqeKey {
	return s.tail.pre.key
}

func (s *singleLru) GetFewActive(n int) []UniqeKey {
	ptr := s.head
	res := make([]UniqeKey, 0, n)

	for i := 0; i < n && ptr.next != s.tail; i++ {
		res = append(res, ptr.next.key)
		ptr = ptr.next
	}
	return res
}
func (s *singleLru) GetFewInactive(n int) []UniqeKey {
	ptr := s.tail
	res := make([]UniqeKey, 0, n)

	for i := 0; i < n && ptr.pre != s.head; i++ {
		res = append(res, ptr.pre.key)
		ptr = ptr.pre
	}
	return res
}

func (s *singleLru) Size() int {
	return s.size
}

func NewsingleLru() *singleLru {
	s := &singleLru{
		head:       new(sNode),
		tail:       new(sNode),
		bufferKeys: make(map[UniqeKey]*sNode, 64),
	}
	s.head.next = s.tail
	s.tail.pre = s.head
	return s
}

type segmentLru struct {
	inactive  *singleLru
	active    *singleLru
	head      *sNode
	tail      *sNode
	activeNum int
}

func (s *segmentLru) Put(key UniqeKey) {

	if _, inactiveOk := s.inactive.get(key); inactiveOk {
		s.inactive.remove(key)
		s.active.Put(key)
		if s.active.Size() > s.activeNum {
			uqikey := s.active.Victim()
			s.inactive.Put(uqikey)
		}
	} else if _, activeOk := s.active.get(key); activeOk {
		s.active.remove(key)
		s.active.Put(key)

	} else {

		s.inactive.Put(key)
	}

}

func (s *segmentLru) Victim() UniqeKey {

	tail := s.inactive.tail
	victim := tail.pre
	pre := victim.pre
	next := victim.next
	pre.next = next
	next.pre = pre
	victim.pre = nil
	victim.next = nil
	return victim.key
}
func (s *segmentLru) GetVictim() UniqeKey {
	return s.inactive.tail.pre.key
}

func (s *segmentLru) GetFewActive(limit int) []UniqeKey {
	head := s.active.head

	var ukeys []UniqeKey = make([]UniqeKey, 0, limit)
	n := head.next
	num := 0
	for n != nil && uintptr(unsafe.Pointer(n)) != uintptr(unsafe.Pointer(s.active.tail)) && num < limit {

		ukeys = append(ukeys, n.key)
		n = n.next
		num++
	}
	return ukeys
}

func (s *segmentLru) GetFewInactive(num int) []UniqeKey {
	var res []UniqeKey = make([]UniqeKey, 0, num)
	n := s.inactive.tail.pre
	for i := 0; i < num; i++ {
		if n == nil || uintptr(unsafe.Pointer(n)) == uintptr(unsafe.Pointer(s.inactive.head)) {
			break
		}
		res = append(res, n.key)
		n = n.pre

	}
	return res
}

func (s *segmentLru) Size() int {
	return s.active.Size() + s.inactive.Size()
}

func (s *segmentLru) GetActiveSize() int {
	return s.active.Size()
}
func (s *segmentLru) GetInactiveSize() int {
	return s.inactive.Size()
}
func NewSegmentLru(activeNum int) *segmentLru {
	s := &segmentLru{
		activeNum: activeNum,
		inactive:  NewsingleLru(),
		active:    NewsingleLru(),
	}
	s.head = s.active.head
	s.tail = s.inactive.tail
	return s
}

func (s *segmentLru) RemoveNodeByKey(key UniqeKey) {
	if _, ok := s.active.bufferKeys[key]; ok {
		s.active.remove(key)
		delete(s.active.bufferKeys, key)
	} else if _, ok := s.inactive.bufferKeys[key]; ok {
		s.inactive.remove(key)
		delete(s.inactive.bufferKeys, key)
	}
}
