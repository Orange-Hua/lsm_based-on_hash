package storage

import (
	"kv/internal"
	"kv/iterator"
	"os"
	"sync"
	"sync/atomic"
)

// LRU算法
type tableCache struct {
	dirname string
	fs      internal.FileSystem
	opt     *ReadOpt

	size int

	mu    sync.Mutex
	nodes map[uint64]*tableCacheNode
	dummy tableCacheNode
}

func newTableCache(dirname string, fs internal.FileSystem, o *ReadOpt, size int) *tableCache {
	t := tableCache{}
	t.init(dirname, fs, o, size)
	return &t
}

func (c *tableCache) init(dirname string, fs internal.FileSystem, o *ReadOpt, size int) {
	c.dirname = dirname
	c.fs = fs
	c.opt = o
	c.size = size
	c.nodes = make(map[uint64]*tableCacheNode)
	c.dummy.next = &c.dummy
	c.dummy.prev = &c.dummy

}
func (c *tableCache) Find(fileNum uint64, ikey []byte, opt *ReadOpt) (iterator.Iterator, error) {

	return c.find(fileNum, ikey, opt)
}
func (c *tableCache) find(fileNum uint64, ikey []byte, opt *ReadOpt) (iterator.Iterator, error) {
	//不在内存，加载进来，增加cache的引用计数，
	node := c.findNode(fileNum)
	x := <-node.result
	if x.err != nil {
		c.mu.Lock()
		node.refCount.Add(-1)
		if node.refCount.Load() == 0 {
			go node.release()
		}
		c.mu.Unlock()

		// Try loading the table again; the error may be transient.
		go node.load(c, opt)
		return nil, x.err
	}
	node.result <- x
	return &tableCacheIter{Iterator: x.reader.Find(ikey), cache: c, node: node}, nil

}
func (c *tableCache) releaseNode(n *tableCacheNode) {
	if n == nil {
		return
	}
	delete(c.nodes, n.fileNum)
	n.next.prev = n.prev
	n.prev.next = n.next
	n.refCount.Add(-1)
	if n.refCount.Load() <= 0 {
		go n.release()
	}

}

func (c *tableCache) findNode(fileNum uint64) *tableCacheNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	node := c.nodes[fileNum]
	if node == nil {
		count := atomic.Int32{}
		node = &tableCacheNode{
			fileNum:  fileNum,
			refCount: &count,
			result:   make(chan tableReaderOrError, 1),
		}
		node.refCount.Add(1)
		c.nodes[fileNum] = node
		if len(c.nodes) > c.size {
			// Release the tail node.
			c.releaseNode(c.dummy.prev)
		}
		go node.load(c, nil)

	} else {
		node.next.prev = node.prev
		node.prev.next = node.next
	}
	node.next = c.dummy.next
	node.prev = &c.dummy
	node.next.prev = node
	c.dummy.next = node
	node.refCount.Add(1)
	return node
}

func (c *tableCache) evict(fileNum uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if n := c.nodes[fileNum]; n != nil {
		c.releaseNode(n)
		delete(c.nodes, fileNum)
	}

}

type tableReaderOrError struct {
	reader *Reader
	err    error
}

type tableCacheNode struct {
	fileNum uint64
	result  chan tableReaderOrError

	// The remaining fields are protected by the tableCache mutex.

	next, prev *tableCacheNode
	refCount   *atomic.Int32
}

func (n *tableCacheNode) load(c *tableCache, opt *ReadOpt) {
	f, err := c.fs.Open(dbFilename(c.dirname, fileTypeTable, n.fileNum))
	if os.IsNotExist(err) {
		f, err = c.fs.Open(dbFilename(c.dirname, fileTypeOldFashionedTable, n.fileNum))
	}
	if err != nil {
		n.result <- tableReaderOrError{err: err}
		return
	}
	o := opt
	if o == nil {
		o = c.opt
	}
	n.result <- tableReaderOrError{reader: NewReader(f, o)}
}

func (n *tableCacheNode) release() {
	x := <-n.result
	if x.err != nil {
		return
	}
	x.reader.Close()
	//log.Printf("节点%d被驱逐\r\n", n.fileNum)
}

type tableCacheIter struct {
	iterator.Iterator
	cache    *tableCache
	node     *tableCacheNode
	closeErr error
	closed   bool
}

func (i *tableCacheIter) Close() error {
	i.cache.mu.Lock()
	defer i.cache.mu.Unlock()
	if i.closed {
		return i.closeErr
	}
	i.closed = true

	i.node.refCount.Add(-1)
	if i.node.refCount.Load() == 0 {
		//i.cache.releaseNode(i.node)
		go i.node.release()

	}

	i.closeErr = i.Iterator.Close()
	return i.closeErr
}

func (i *tableCacheIter) Key() []byte {
	return i.Iterator.Key()
}
