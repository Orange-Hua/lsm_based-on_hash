package iterator

import "kv/internal"

type Pair struct {
	Key   []byte
	Value []byte
}
type Iterator interface {
	// Next moves the iterator to the next key/value pair.
	// It returns whether the iterator is exhausted.
	First() bool
	Next() bool

	// Key returns the key of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Value() []byte

	// Close closes the iterator and returns any accumulated error. Exhausting
	// all the key/value pairs in a table is not considered to be an error.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the iterator has been closed.
	Close() error
}

type concatenatingIter struct {
	iters []Iterator
	err   error
}

/*
tableIterator和blockIterator的连用就是Concatenat
*/
func NewConcatenatingIterator(iters ...Iterator) Iterator {
	if len(iters) == 0 {
		return iters[0]
	}
	// return &concatenatingIter{
	// 	iters: iters,
	// }
	return nil
}

// func (c *concatenatingIter) Next() bool {
// 	if c.err != nil {
// 		return false
// 	}
// 	for len(c.iters) > 0 {
// 		if c.iters[0].Next() {
// 			return true
// 		}
// 		if err := c.iters[0].Close(); err != nil {
// 			c.err = err
// 			return false
// 		}
// 		c.iters = c.iters[1:]
// 	}
// }

// func (c *concatenatingIter) Key() []byte {
// 	if len(c.iters) == 0 || c.err != nil {
// 		return nil
// 	}
// 	return c.iters[0].Key()
// }
// func (c *concatenatingIter) Value() []byte {
// 	if len(c.iters) == 0 || c.err != nil {
// 		return nil
// 	}
// 	return c.iters[0].Value()
// }
// func (c *concatenatingIter) Close() error {
// 	for _, t := range c.iters {
// 		err := t.Close()
// 		if c.err == nil {
// 			c.err = err
// 		}
// 	}
// 	c.iters = nil
// 	return c.err
// }

func NewMergingIterator(cmp internal.Comparer, iters ...Iterator) Iterator {
	// if len(iters) == 1 {
	// 	return iters[0]
	// }
	// return &mergingIter{
	// 	iters: iters,
	// 	cmp:   cmp,
	// 	keys:  make([][]byte, len(iters)),
	// 	index: -1,
	// }
	return nil
}

/*
参与压缩的所有tableIterator，从key最小到key最大多路归并排序
*/
type mergingIter struct {
	// iters are the input iterators. An element is set to nil when that
	// input iterator is done.
	iters []Iterator
	err   error
	cmp   internal.Comparer
	// keys[i] is the current key for iters[i].
	keys [][]byte
	// index is:
	//   - -2 if the mergingIter is done,
	//   - -1 if the mergingIter has not yet started,
	//   - otherwise, the index (in iters and in keys) of the smallest key.
	index int
}

func (m *mergingIter) Next() bool {
	if m.err != nil {
		return false
	}
	return false
}
