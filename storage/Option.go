package storage

import (
	"bytes"
	"encoding/binary"
	"kv/internal"
)

type ReadOpt struct {
	verifyChecksums bool
	fp              internal.FilterPolicy
	dataCmp         internal.SeparatorByteComparer
	kcmp            internal.SeparatorByteComparer
}

type WriteOpt struct {
	compression internal.Compression
}

type byteComparer struct {
}

func (bc *byteComparer) Compare(a []byte, b []byte) int {
	return bytes.Compare(a, b)
}

type Comparer struct {
}

var decmp Comparer

func (c *Comparer) CompareKey(a []byte, b []byte) int {
	akey, bkey := binary.BigEndian.Uint64(a), binary.BigEndian.Uint64(b)

	if akey > bkey {
		return -1
	}
	if akey < bkey {
		return 1
	}
	return 0
}

func (c *Comparer) CompareConflictKey(a []byte, b []byte) int {
	akey, bkey := binary.BigEndian.Uint64(a[8:]), binary.BigEndian.Uint64(b[8:])

	if akey > bkey {
		return -1
	}
	if akey < bkey {
		return 1
	}
	return 0
}
func (c *Comparer) Compare(a []byte, b []byte) int {
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
func (c *Comparer) Name() string {
	return ""
}

func (c *Comparer) AppendSeparator(dst, a, b []byte) []byte {
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
func NewReadOpt(verifyChecksums bool) *ReadOpt {
	return &ReadOpt{verifyChecksums: verifyChecksums}
}
