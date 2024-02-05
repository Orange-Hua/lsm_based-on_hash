package record

import (
	"encoding/binary"
	"errors"
	"io"
	"kv/crc"
)

// const (
// 	fullChunkType   = 1
// 	firstChunkType  = 2
// 	middleChunkType = 3
// 	lastChunkType   = 4
// )

// const (
// 	blockSize     = 32 * 1024
// 	blockSizeMask = blockSize - 1
// 	headerSize    = 7
// )

// var (
// 	// ErrNotAnIOSeeker is returned if the io.Reader underlying a Reader does not implement io.Seeker.
// 	ErrNotAnIOSeeker = errors.New("leveldb/record: reader does not implement io.Seeker")

// 	// ErrNoLastRecord is returned if LastRecordOffset is called and there is no previous record.
// 	ErrNoLastRecord = errors.New("leveldb/record: no last record exists")
// )

// type flusher interface {
// 	Flush() error
// }
// type Reader struct {
// 	r          io.Reader
// 	seq        int
// 	i, j       int
// 	n          int  //buf中现有数据的长度，除了lastblock,剩下都为blocksize
// 	startd     bool //读record开始
// 	recovering bool //正从错误中恢复
// 	last       bool
// 	err        error
// 	buf        [blockSize]byte
// }

// // 从文件中读入chunk
// /*
// 	一个块中有多个chunk，因此一个块中，无论怎么读，都是能最终读到完整的chunk的
// 	先读一个整块，
// 	然后视chunk尺寸多次读chunk，或一个块只有一个chunk，
// 	n是以blocksize为尺寸，读出来的数据长度，非最后一个块正好是blockSize，最后一个块n<=blockSize
// 	i，j是读出来的chunk的长度的左右边界，所以j+读出来的chunk长度正常应该是<=n的

// */
// func (r *Reader) nextChunk(wantFirst bool) error {

// 	for {
// 		//读出来的chunk，加上想要读下一个chunk的头长度，超过块大小，说明此块读完了，读下一个块
// 		if r.j+headerSize <= r.n {
// 			checkSum := binary.LittleEndian.Uint32(r.buf[r.j : r.j+4])
// 			length := binary.LittleEndian.Uint16(r.buf[r.j+4 : r.j+6])
// 			chunkType := r.buf[r.j+6]
// 			if r.j+headerSize+int(length) > r.n {
// 				if r.recovering {
// 					r.Recover()
// 					continue
// 				}
// 				return errors.New("chunk数据长度大于块长度")
// 			}
// 			r.i = r.j
// 			r.j += headerSize + int(length)
// 			if actualCheckSum := crc.New(r.buf[r.i-1 : r.j]).Value(); actualCheckSum != checkSum {
// 				if r.recovering {
// 					r.Recover()
// 					continue
// 				}
// 				return errors.New("校验和不对")
// 			}
// 			if wantFirst {
// 				if chunkType != fullChunkType && chunkType != firstChunkType {
// 					continue
// 				}
// 			}
// 			r.last = chunkType == fullChunkType || chunkType == lastChunkType
// 			r.recovering = false
// 			return nil
// 		}
// 		if r.n < blockSize && r.startd {
// 			if r.j != r.n {
// 				return io.ErrUnexpectedEOF
// 			}
// 			return io.EOF
// 		}
// 		n, err := io.ReadFull(r.r, r.buf[:])
// 		if err != nil && err != io.ErrUnexpectedEOF {
// 			return err
// 		}
// 		r.i, r.j, r.n = 0, 0, n
// 	}

// }

// // 读下一个chunk，即下一个记录的首个块
// func (r *Reader) Next() (io.Reader, error) {
// 	r.seq++
// 	if r.err != nil {
// 		return nil, r.err
// 	}
// 	err := r.nextChunk(true)
// 	if err != nil {
// 		r.err = err
// 		return nil, err
// 	}
// 	r.startd = true
// 	return singleReader{r, r.seq}, nil
// }

// func (r *Reader) Recover() {
// 	if r.err == nil {
// 		return
// 	}
// 	r.recovering = true
// 	r.err = nil
// 	//忽略块剩余的数据
// 	r.i, r.j, r.last = r.n, r.n, false
// 	//所有原有r.seq号对应的singleReader全部作废
// 	r.seq++

// }

// func (r *Reader) SeekRecord(offset int64) error {
// 	r.seq++
// 	if r.err != nil {
// 		return r.err
// 	}
// 	s, ok := r.r.(io.Seeker)
// 	if !ok {
// 		return ErrNotAnIOSeeker
// 	}

// 	c := int(offset &^ blockSizeMask)
// 	//相较于io.seekStart，偏移量增加第一个参数
// 	if _, r.err = s.Seek(offset&^blockSizeMask, io.SeekStart); r.err != nil {
// 		return r.err
// 	}
// 	r.i, r.j, r.n = 0, 0, 0 //设置0，除去本来是r.n<blockSize的影响
// 	r.startd, r.recovering, r.last = false, false, false
// 	if r.err = r.nextChunk(false); r.err != nil { //更新r.n
// 		return r.err
// 	}
// 	//调用next，返回请求偏移处的数据块，在该数据块上读chunk
// 	r.i, r.j = c, c
// 	return nil
// }

// type singleReader struct {
// 	r   *Reader
// 	seq int
// }

// // 该reader,读一个record,能从first一直读到last
// func (s singleReader) Read(p []byte) (int, error) {

// 	r := s.r
// 	if s.seq < r.seq {
// 		return 0, errors.New("singleReader is stale")
// 	}
// 	if r.err != nil {
// 		return 0, r.err
// 	}
// 	for r.i == r.j {
// 		if r.last {
// 			return 0, io.EOF
// 		}
// 		if r.err = r.nextChunk(false); r.err != nil {
// 			return 0, r.err
// 		}
// 	}
// 	n := copy(p, r.buf[r.i:r.j])
// 	r.i += n
// 	return n, nil
// }

// // 无论写和读，都要考虑，缓冲中现有的数据
// type Writer struct {
// 	w   io.Writer
// 	seq int
// 	f   flusher
// 	i   int //块起点
// 	n   int //buf中已写入数据的长度

// 	baseOffset       int64
// 	lastRecordOffset int64

// 	blockNumber int64
// 	first       bool //第一个block
// 	pending     bool //当前记录正在写入标志

// 	err error
// 	buf [blockSize]byte
// }

// type Record struct {
// 	w   io.Writer
// 	seq int
// 	f   flusher
// 	i   int //块起点
// 	n   int //buf中已写入数据的长度

// 	baseOffset       int64
// 	lastRecordOffset int64

// 	blockNumber int64
// 	first       bool //第一个block
// 	pending     bool //当前记录正在写入标志

// 	err error
// 	buf [blockSize]byte
// }

// // 通过last和first标记blocktype
// func (w *Writer) fillHeader(last bool) {
// 	if last {
// 		if w.first {
// 			w.buf[w.i+6] = fullChunkType
// 		} else {
// 			w.buf[w.i+6] = lastChunkType
// 		}
// 	} else {
// 		if w.first {
// 			w.buf[w.i+6] = firstChunkType
// 		} else {
// 			w.buf[w.i+6] = middleChunkType
// 		}
// 	}
// 	binary.LittleEndian.PutUint32(w.buf[w.i+0:w.i+4], crc.New(w.buf[w.i+6:w.j]).Value())
// 	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-headerSize))

// }

// func (w *Writer) Write() error {
// 	var n int
// 	n, err := w.w.Write(w.buf[0:w.n])
// 	if n < w.n && err == nil {
// 		err = io.ErrShortWrite
// 	}
// 	if err != nil {
// 		w.err = err
// 		if n > 0 && n < w.n {
// 			copy(w.buf[0:w.n-n], w.buf[n:w.n])
// 		}

// 		w.i = w.n - n
// 		w.n = w.i + headerSize

// 		return err
// 	}

// 	w.i = 0
// 	w.n = w.i + headerSize
// 	return nil
// }
// func (w *Writer) writeBlock() {

// 	//w.written = 0
// 	w.Write()
// 	w.blockNumber++
// }

// // 完成当前正在写入的记录
// func (w *Writer) writePending() error {

// 	if w.err != nil {
// 		return w.err
// 	}
// 	if w.pending {
// 		w.fillHeader(true)
// 		w.pending = false
// 	}
// 	n, err := w.w.Write(w.buf[0:w.n])
// 	if err != nil {
// 		w.err = err
// 		return w.err
// 	}
// 	if n < w.n {
// 		w.err = io.ErrShortWrite
// 		copy(w.buf[0:w.n-n], w.buf[n:w.n])
// 		w.i = w.n - n
// 		w.n = w.n - n
// 		return w.err
// 	}
// 	w.i = 0
// 	w.n = w.i + headerSize
// 	return nil
// }

// // Close finishes the current record and closes the writer.
// func (w *Writer) Close() error {
// 	w.seq++
// 	w.writePending()
// 	if w.err != nil {
// 		return w.err
// 	}
// 	w.err = errors.New("leveldb/record: closed Writer")
// 	return nil
// }

// func (w *Writer) Flush() error {
// 	w.seq++
// 	w.writePending()
// 	if w.err != nil {
// 		return w.err
// 	}

// 	if w.f != nil {
// 		w.err = w.f.Flush()
// 		return w.err
// 	}
// 	return nil
// }

// func (w *Writer) LastRecordOffset() (int64, error) {
// 	if w.err != nil {
// 		return 0, w.err
// 	}
// 	if w.lastRecordOffset < 0 {
// 		return 0, ErrNoLastRecord
// 	}
// 	return w.lastRecordOffset, nil
// }

// func NewWriter(w io.Writer) *Writer {
// 	f, _ := w.(flusher)

// 	var o int64
// 	if s, ok := w.(io.Seeker); ok {
// 		var err error
// 		if o, err = s.Seek(0, io.SeekCurrent); err != nil {
// 			o = 0
// 		}
// 	}
// 	return &Writer{
// 		w:                w,
// 		f:                f,
// 		baseOffset:       o,
// 		lastRecordOffset: -1,
// 	}
// }

// // 对上一个记录完成封装，返回writer写下一个记录的第一个块first
// func (w *Writer) Next() (io.Writer, error) {
// 	w.seq++
// 	if w.err != nil {
// 		return nil, w.err
// 	}
// 	if w.pending {
// 		w.fillHeader(true)
// 	}

// 	//下一个record的写入偏移
// 	w.i = w.n
// 	w.j = w.i + headerSize
// 	//用0填充
// 	if w.n > blockSize {
// 		for k := w.i; k < blockSize; k++ {
// 			w.buf[k] = 0
// 		}
// 		w.writeBlock()
// 		if w.err != nil {
// 			return nil, w.err
// 		}
// 	}

// 	w.lastRecordOffset = w.baseOffset + w.blockNumber*blockSize + int64(w.i)
// 	w.first = true
// 	w.pending = true //正在写入标志
// 	return singleWriter{w, w.seq}, nil
// }

// type singleWriter struct {
// 	w   *Writer
// 	seq int
// }

// //record是块为单位，连续写入

// /*
// 一个记录块中是，last block+first block
// */
// func (s singleWriter) Write(p []byte) (int, error) {
// 	w := s.w
// 	if s.seq != w.seq {
// 		return 0, errors.New("leveldb/record: stale writer")
// 	}
// 	if w.err != nil {
// 		return 0, w.err
// 	}
// 	n0 := 0
// 	/*
// 		buf里还有数据，写入p的一部分，凑成buf写入，够buf了，直接写入

//		*/
//		for len(p) > 0 && w.err == nil {
//			//一次写入j往后的
//			if w.n == blockSize {
//				w.fillHeader(false)
//				w.writeBlock()
//				w.first = false
//			}
//			n := copy(w.buf[w.n:], p)
//			w.n += n
//			n0 += n
//			p = p[n:]
//		}
//		return n0, w.err
//	}
const (
	fullChunkType   = 1
	firstChunkType  = 2
	middleChunkType = 3
	lastChunkType   = 4
)

const (
	blockSize     = 32 * 1024
	blockSizeMask = blockSize - 1
	headerSize    = 7
)

var (
	// ErrNotAnIOSeeker is returned if the io.Reader underlying a Reader does not implement io.Seeker.
	ErrNotAnIOSeeker = errors.New("leveldb/record: reader does not implement io.Seeker")

	// ErrNoLastRecord is returned if LastRecordOffset is called and there is no previous record.
	ErrNoLastRecord = errors.New("leveldb/record: no last record exists")
)

type flusher interface {
	Flush() error
}

// Reader reads records from an underlying io.Reader.
type Reader struct {
	// r is the underlying reader.
	r io.Reader
	// seq is the sequence number of the current record.
	seq int
	// buf[i:j] is the unread portion of the current chunk's payload.
	// The low bound, i, excludes the chunk header.
	i, j int
	// n is the number of bytes of buf that are valid. Once reading has started,
	// only the final block can have n < blockSize.
	n int
	// started is whether Next has been called at all.
	started bool
	// recovering is true when recovering from corruption.
	recovering bool
	// last is whether the current chunk is the last chunk of the record.
	last bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [blockSize]byte
}

// NewReader returns a new reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}

// nextChunk sets r.buf[r.i:r.j] to hold the next chunk's payload, reading the
// next block into the buffer if necessary.
func (r *Reader) nextChunk(wantFirst bool) error {
	for {
		if r.j+headerSize <= r.n {
			checksum := binary.LittleEndian.Uint32(r.buf[r.j+0 : r.j+4])
			length := binary.LittleEndian.Uint16(r.buf[r.j+4 : r.j+6])
			chunkType := r.buf[r.j+6]

			if checksum == 0 && length == 0 && chunkType == 0 {
				if wantFirst || r.recovering {
					// Skip the rest of the block, if it looks like it is all
					// zeroes. This is common if the record file was created
					// via mmap.
					//
					// Set r.err to be an error so r.Recover actually recovers.
					r.err = errors.New("leveldb/record: block appears to be zeroed")
					r.Recover()
					continue
				}
				return errors.New("leveldb/record: invalid chunk")
			}

			r.i = r.j + headerSize
			r.j = r.j + headerSize + int(length)
			if r.j > r.n {
				if r.recovering {
					r.Recover()
					continue
				}
				return errors.New("leveldb/record: invalid chunk (length overflows block)")
			}
			if checksum != crc.New(r.buf[r.i-1:r.j]).Value() {
				if r.recovering {
					r.Recover()
					continue
				}
				return errors.New("leveldb/record: invalid chunk (checksum mismatch)")
			}
			if wantFirst {
				if chunkType != fullChunkType && chunkType != firstChunkType {
					continue
				}
			}
			r.last = chunkType == fullChunkType || chunkType == lastChunkType
			r.recovering = false
			return nil
		}
		if r.n < blockSize && r.started {
			if r.j != r.n {
				return io.ErrUnexpectedEOF
			}
			return io.EOF
		}
		n, err := io.ReadFull(r.r, r.buf[:])
		if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		r.i, r.j, r.n = 0, 0, n
	}
}

// Next returns a reader for the next record. It returns io.EOF if there are no
// more records. The reader returned becomes stale after the next Next call,
// and should no longer be used.
func (r *Reader) Next() (io.Reader, error) {
	r.seq++
	if r.err != nil {
		return nil, r.err
	}
	r.i = r.j
	r.err = r.nextChunk(true)
	if r.err != nil {
		return nil, r.err
	}
	r.started = true
	return singleReader{r, r.seq}, nil
}

// Recover clears any errors read so far, so that calling Next will start
// reading from the next good 32KiB block. If there are no such blocks, Next
// will return io.EOF. Recover also marks the current reader, the one most
// recently returned by Next, as stale. If Recover is called without any
// prior error, then Recover is a no-op.
func (r *Reader) Recover() {
	if r.err == nil {
		return
	}
	r.recovering = true
	r.err = nil
	// Discard the rest of the current block.
	r.i, r.j, r.last = r.n, r.n, false
	// Invalidate any outstanding singleReader.
	r.seq++
	return
}

// SeekRecord seeks in the underlying io.Reader such that calling r.Next
// returns the record whose first chunk header starts at the provided offset.
// Its behavior is undefined if the argument given is not such an offset, as
// the bytes at that offset may coincidentally appear to be a valid header.
//
// It returns ErrNotAnIOSeeker if the underlying io.Reader does not implement
// io.Seeker.
//
// SeekRecord will fail and return an error if the Reader previously
// encountered an error, including io.EOF. Such errors can be cleared by
// calling Recover. Calling SeekRecord after Recover will make calling Next
// return the record at the given offset, instead of the record at the next
// good 32KiB block as Recover normally would. Calling SeekRecord before
// Recover has no effect on Recover's semantics other than changing the
// starting point for determining the next good 32KiB block.
//
// The offset is always relative to the start of the underlying io.Reader, so
// negative values will result in an error as per io.Seeker.
func (r *Reader) SeekRecord(offset int64) error {
	r.seq++
	if r.err != nil {
		return r.err
	}

	s, ok := r.r.(io.Seeker)
	if !ok {
		return ErrNotAnIOSeeker
	}

	// Only seek to an exact block offset.
	c := int(offset & blockSizeMask)
	if _, r.err = s.Seek(offset&^blockSizeMask, io.SeekStart); r.err != nil {
		return r.err
	}

	// Clear the state of the internal reader.
	r.i, r.j, r.n = 0, 0, 0
	r.started, r.recovering, r.last = false, false, false
	if r.err = r.nextChunk(false); r.err != nil {
		return r.err
	}

	// Now skip to the offset requested within the block. A subsequent
	// call to Next will return the block at the requested offset.
	r.i, r.j = c, c

	return nil
}

type singleReader struct {
	r   *Reader
	seq int
}

func (x singleReader) Read(p []byte) (int, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("leveldb/record: stale reader")
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		if r.err = r.nextChunk(false); r.err != nil {
			return 0, r.err
		}
	}
	n := copy(p, r.buf[r.i:r.j])
	r.i += n
	return n, nil
}

// Writer writes records to an underlying io.Writer.
type Writer struct {
	// w is the underlying writer.
	w io.Writer
	// seq is the sequence number of the current record.
	seq int
	// f is w as a flusher.
	f flusher
	// buf[i:j] is the bytes that will become the current chunk.
	// The low bound, i, includes the chunk header.
	i, j int
	// buf[:written] has already been written to w.
	// written is zero unless Flush has been called.
	written int
	// baseOffset is the base offset in w at which writing started. If
	// w implements io.Seeker, it's relative to the start of w, 0 otherwise.
	baseOffset int64
	// blockNumber is the zero based block number currently held in buf.
	blockNumber int64
	// lastRecordOffset is the offset in w where the last record was
	// written (including the chunk header). It is a relative offset to
	// baseOffset, thus the absolute offset of the last record is
	// baseOffset + lastRecordOffset.
	lastRecordOffset int64
	// first is whether the current chunk is the first chunk of the record.
	first bool
	// pending is whether a chunk is buffered but not yet written.
	pending bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [blockSize]byte
}

// NewWriter returns a new Writer.
func NewWriter(w io.Writer) *Writer {
	f, _ := w.(flusher)

	var o int64
	if s, ok := w.(io.Seeker); ok {
		var err error
		if o, err = s.Seek(0, io.SeekCurrent); err != nil {
			o = 0
		}
	}
	return &Writer{
		w:                w,
		f:                f,
		baseOffset:       o,
		lastRecordOffset: -1,
	}
}

// fillHeader fills in the header for the pending chunk.
func (w *Writer) fillHeader(last bool) {
	if w.i+headerSize > w.j || w.j > blockSize {
		panic("leveldb/record: bad writer state")
	}
	if last {
		if w.first {
			w.buf[w.i+6] = fullChunkType
		} else {
			w.buf[w.i+6] = lastChunkType
		}
	} else {
		if w.first {
			w.buf[w.i+6] = firstChunkType
		} else {
			w.buf[w.i+6] = middleChunkType
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.i+0:w.i+4], crc.New(w.buf[w.i+6:w.j]).Value())
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-headerSize))
}

// writeBlock writes the buffered block to the underlying writer, and reserves
// space for the next chunk's header.
func (w *Writer) writeBlock() {
	_, w.err = w.w.Write(w.buf[w.written:])
	w.i = 0
	w.j = headerSize
	w.written = 0
	w.blockNumber++
}

// writePending finishes the current record and writes the buffer to the
// underlying writer.
func (w *Writer) writePending() {
	if w.err != nil {
		return
	}
	if w.pending {
		w.fillHeader(true)
		w.pending = false
	}
	_, w.err = w.w.Write(w.buf[w.written:w.j])
	w.written = w.j
}

// Close finishes the current record and closes the writer.
func (w *Writer) Close() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	w.err = errors.New("leveldb/record: closed Writer")
	return nil
}

// Flush finishes the current record, writes to the underlying writer, and
// flushes it if that writer implements interface{ Flush() error }.
func (w *Writer) Flush() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	if w.f != nil {
		w.err = w.f.Flush()
		return w.err
	}
	return nil
}

// Next returns a writer for the next record. The writer returned becomes stale
// after the next Close, Flush or Next call, and should no longer be used.
func (w *Writer) Next() (io.Writer, error) {
	w.seq++
	if w.err != nil {
		return nil, w.err
	}
	if w.pending {
		w.fillHeader(true)
	}
	w.i = w.j
	w.j = w.j + headerSize
	// Check if there is room in the block for the header.
	if w.j > blockSize {
		// Fill in the rest of the block with zeroes.
		for k := w.i; k < blockSize; k++ {
			w.buf[k] = 0
		}
		w.writeBlock()
		if w.err != nil {
			return nil, w.err
		}
	}
	w.lastRecordOffset = w.baseOffset + w.blockNumber*blockSize + int64(w.i)
	w.first = true
	w.pending = true
	return singleWriter{w, w.seq}, nil
}

// LastRecordOffset returns the offset in the underlying io.Writer of the last
// record so far - the one created by the most recent Next call. It is the
// offset of the first chunk header, suitable to pass to Reader.SeekRecord.
//
// If that io.Writer also implements io.Seeker, the return value is an absolute
// offset, in the sense of io.SeekStart, regardless of whether the io.Writer
// was initially at the zero position when passed to NewWriter. Otherwise, the
// return value is a relative offset, being the number of bytes written between
// the NewWriter call and any records written prior to the last record.
//
// If there is no last record, i.e. nothing was written, LastRecordOffset will
// return ErrNoLastRecord.
func (w *Writer) LastRecordOffset() (int64, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.lastRecordOffset < 0 {
		return 0, ErrNoLastRecord
	}
	return w.lastRecordOffset, nil
}

type singleWriter struct {
	w   *Writer
	seq int
}

func (x singleWriter) Write(p []byte) (int, error) {
	w := x.w
	if w.seq != x.seq {
		return 0, errors.New("leveldb/record: stale writer")
	}
	if w.err != nil {
		return 0, w.err
	}
	n0 := len(p)
	for len(p) > 0 {
		// Write a block, if it is full.
		if w.j == blockSize {
			w.fillHeader(false)
			w.writeBlock()
			if w.err != nil {
				return 0, w.err
			}
			w.first = false
		}
		// Copy bytes into the buffer.
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}
