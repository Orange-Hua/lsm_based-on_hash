package internal

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type GPool struct {
	mode       int
	taskNum    int
	tasks      []chan func()
	taskBuffer *list.List
	index      int
	rwmutex    sync.RWMutex
	notify     chan struct{}
	curTaskNum atomic.Int32
	cond       *sync.Cond
	condMutex  sync.Mutex
}

const (
	Asy int = 0
	Syn int = 1
)

func NewGPool(taskNum, mode int) *GPool {
	gpool := GPool{}
	gpool.taskNum = taskNum
	gpool.mode = mode
	gpool.tasks = make([]chan func(), gpool.taskNum)
	gpool.cond = &sync.Cond{L: &gpool.rwmutex}
	gpool.taskBuffer = list.New()
	for i := 0; i < gpool.taskNum; i++ {

		gpool.tasks[i] = make(chan func(), 1)
	}
	if mode == Asy {

		for i := 0; i < gpool.taskNum; i++ {
			go gpool.AsyExec(i)
		}
	} else if mode == Syn {

		for i := 0; i < gpool.taskNum; i++ {
			go gpool.SyncExec(i)
		}
	}
	return &gpool
}
func (g *GPool) SynPut(task func()) {
	if g.mode == Asy {
		return
	}
	g.cond.L.Lock()
	for g.chooseBucketToPut() == -1 {
		g.cond.Wait()
	}
	index := g.chooseBucketToPut()

	g.tasks[index] <- task
	g.cond.L.Unlock()

}
func (g *GPool) chooseBucketToPut() int {
	index := -1
	for i := 0; i < g.taskNum; i++ {
		if len(g.tasks[i]) == 0 {
			index = i
			break
		}
	}
	if index == -1 {
		n := 2
		_ = n
	}
	return index
}

func (g *GPool) AsyPut(task func()) {
	if g.mode == Syn {
		return
	}
	g.rwmutex.Lock()
	g.taskBuffer.PushBack(task)

	g.rwmutex.Unlock()
	g.cond.Signal()

}
func (g *GPool) SyncExec(index int) {
	for {
		select {
		case task := <-g.tasks[index]:

			task()

			g.cond.Signal()

		}
	}
}

func (g *GPool) AsyExec(index int) {
	for {
		g.cond.L.Lock()

		for g.taskBuffer.Len() == 0 {
			g.cond.Wait()

		}
		//log.Println("线程池异步执行了")
		v := g.taskBuffer.Front()
		task := g.taskBuffer.Remove(v)
		g.cond.L.Unlock()
		task.(func())()
	}

}
