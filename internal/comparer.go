package internal

import (
	"bytes"
	"encoding/binary"
)

type ByteComparer interface {
	CompareKey(a, b []byte) int
	CompareUniqeKey(a, b []byte) int
	Name() string
}

type SeparatorByteComparer interface {
	ByteComparer
	AppendSeparator(dst, a, b []byte) []byte
}
type Comparer interface {
	// Compare returns -1, 0, or +1 depending on whether a is 'less than',
	// 'equal to' or 'greater than' b. The two arguments can only be 'equal'
	// if their contents are exactly equal. Furthermore, the empty slice
	// must be 'less than' any non-empty slice.
	CompareKey(a, b *InternalKey) int
	CompareUniqeKey(a, b *UniqeKey) int
	// Name returns the name of the comparer.
	//
	// The Level-DB on-disk format stores the comparer name, and opening a
	// database with a different comparer from the one it was created with
	// will result in an error.
	Name() string

	// AppendSeparator appends a sequence of bytes x to dst such that
	// a <= x && x < b, where 'less than' is consistent with Compare.
	// It returns the enlarged slice, like the built-in append function.
	//
	// Precondition: either a is 'less than' b, or b is an empty slice.
	// In the latter case, empty means 'positive infinity', and appending any
	// x such that a <= x will be valid.
	//
	// An implementation may simply be "return append(dst, a...)" but appending
	// fewer bytes will result in smaller tables.
	//
	// For example, if dst, a and b are the []byte equivalents of the strings
	// "aqua", "black" and "blue", then the result may be "aquablb".
	// Similarly, if the arguments were "aqua", "green" and "", then the result
	// may be "aquah".

}

// DefaultComparer is the default implementation of the Comparer interface.
// It uses the natural ordering, consistent with bytes.Compare.
var DefaultComparer Comparer = &cmp{}
var DefaultByteComparer SeparatorByteComparer = &scmp{}
var BytesComparer SeparatorByteComparer = &kcmp{}

type cmp struct {
}

type scmp struct {
}

type kcmp struct {
}

func (c *cmp) CompareUniqeKey(a, b *UniqeKey) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if a.Key == b.Key && a.ConflictKey == b.ConflictKey {
		return 0
	}
	var res bool

	if a.Key != b.Key {
		res = a.Key < b.Key
	} else {
		res = a.ConflictKey < b.ConflictKey
	}

	if res {
		return -1
	}
	return 1
}

func (c *cmp) CompareKey(a, b *InternalKey) int {
	var res bool
	if a.Key != b.Key {
		res = a.Key < b.Key
	}
	if a.Key == b.Key {
		if a.ConflictKey != b.ConflictKey {
			res = a.ConflictKey < b.ConflictKey
		} else if b.SeqNum != a.SeqNum {
			res = b.SeqNum < a.SeqNum
		}
	}
	if b.SeqNum == a.SeqNum {
		return 0
	}
	if res {
		return -1
	}
	return 1
}

func (c *cmp) Name() string {
	return "uniqueComparator"
}

func (s *scmp) Name() string {
	// This string is part of the C++ Level-DB implementation's default file format,
	// and should not be changed.
	return "leveldb.BytewiseComparator"
}

func (s *scmp) AppendSeparator(dst, a, b []byte) []byte {
	i, n := SharedPrefixLen(a, b), len(dst)
	dst = append(dst, a...)
	if len(b) > 0 {
		if i == len(a) {
			return dst
		}
		if i == len(b) {
			panic("a < b is a precondition, but b is a prefix of a")
		}
		if a[i] == 0xff || a[i]+1 >= b[i] {
			// This isn't optimal, but it matches the C++ Level-DB implementation, and
			// it's good enough. For example, if a is "1357" and b is "2", then the
			// optimal (i.e. shortest) result is appending "14", but we append "1357".
			return dst
		}
	}
	i += n
	for ; i < len(dst); i++ {
		if dst[i] != 0xff {
			dst[i]++
			return dst[:i+1]
		}
	}
	return dst
}

// nil最小
func (s *scmp) CompareKey(a []byte, b []byte) int {

	if a == nil || len(a) == 0 {
		return -1
	}
	if b == nil || len(b) == 0 {
		return 1
	}
	akey, bkey := binary.BigEndian.Uint64(a), binary.BigEndian.Uint64(b)
	acKey, bcKey := binary.BigEndian.Uint64(a[8:]), binary.BigEndian.Uint64(b[8:])
	aseqNum, bseqNum := binary.BigEndian.Uint64(a[16:]), binary.BigEndian.Uint64(b[16:])
	var res bool
	if akey == bcKey && acKey == bcKey && aseqNum == bseqNum {
		return 0
	}
	if akey != bkey {
		res = akey < bkey
	}
	if akey == bkey {
		if acKey != bcKey {
			res = acKey < bcKey
		} else if bseqNum != aseqNum {
			res = bseqNum < aseqNum
		}
	}

	if res {
		return -1
	}
	return 1

}

func (s *scmp) CompareUniqeKey(a []byte, b []byte) int {
	if a == nil || len(a) == 0 {
		return -1
	}
	if b == nil || len(b) == 0 {
		return 1
	}

	akey, bkey := binary.BigEndian.Uint64(a), binary.BigEndian.Uint64(b)
	ackey, bckey := binary.BigEndian.Uint64(a[8:]), binary.BigEndian.Uint64(b[8:])
	var res bool
	if akey == bkey && ackey == bckey {
		return 0
	}
	if akey != bkey {
		res = akey < bkey
	} else {
		res = ackey < bckey
	}
	if res {
		return -1
	}
	return 1

}

// SharedPrefixLen returns the largest i such that a[:i] equals b[:i].
// This function can be useful in implementing the Comparer interface.
func SharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func (k *kcmp) AppendSeparator(dst, a, b []byte) []byte {
	return nil
}

func (k *kcmp) Name() string {
	return "kcmp"
}

func (k *kcmp) CompareKey(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (k *kcmp) CompareUniqeKey(a, b []byte) int {
	return bytes.Compare(a, b)
}
