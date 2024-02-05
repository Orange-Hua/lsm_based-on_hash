package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"kv/internal"
	"log"
	"sort"
	"unsafe"
)

// Tags for the versionEdit disk format.
// Tag 8 is no longer used.
const (
	tagComparator     = 1
	tagLogNumber      = 2
	tagNextFileNumber = 3
	tagLastSequence   = 4
	tagCompactPointer = 5
	tagDeletedFile    = 6
	tagNewFile        = 7
	tagPrevLogNumber  = 9
	tagPartNum        = 10
)

type byteReader interface {
	io.ByteReader
	io.Reader
}

type compactPointerEntry struct {
	level int
	key   []byte
}

type deletedFileEntry struct {
	level       int
	subPart     int
	fileNum     uint64
	logicDelete int
}

type newFileEntry struct {
	level   int
	subPart int
	meta    fileMetadata
}

var errCorruptManifest = errors.New("leveldb: corrupt manifest")

type VersionEdit struct {
	comparatorName  string
	partNum         int
	logNumber       uint64
	prelogNumber    uint64
	nextFileNumber  uint64
	lastSequence    uint64
	compactPointers []compactPointerEntry
	deletedFiles    map[deletedFileEntry]bool // A set of deletedFileEntry values.
	newFiles        []newFileEntry
}

func (v *VersionEdit) decode(r io.Reader) error {
	br, ok := r.(byteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	d := versionEditDecoder{br}
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch tag {

		case tagComparator:
			s, err := d.readBytes()
			if err != nil {
				return err
			}
			v.comparatorName = string(s)

		case tagLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.logNumber = n

		case tagNextFileNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.nextFileNumber = n

		case tagLastSequence:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.lastSequence = n
		case tagPartNum:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.partNum = int(n)
		case tagCompactPointer:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			key, err := d.readBytes()
			if err != nil {
				return err
			}
			v.compactPointers = append(v.compactPointers, compactPointerEntry{level, key})

		case tagDeletedFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			subPart, err := d.readUvarint()
			if err != nil {
				return err
			}
			fileNum, err := d.readUvarint()
			if err != nil {
				return err
			}
			logicDelete, err := d.readUvarint()
			if err != nil {
				return err
			}
			if v.deletedFiles == nil {
				v.deletedFiles = make(map[deletedFileEntry]bool)
			}
			v.deletedFiles[deletedFileEntry{level: level, subPart: int(subPart), fileNum: fileNum, logicDelete: int(logicDelete)}] = true

		case tagNewFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			subPart, err := d.readUvarint()
			if err != nil {
				return err
			}
			fileNum, err := d.readUvarint()
			if err != nil {
				return err
			}
			size, err := d.readUvarint()
			if err != nil {
				return err
			}
			smallest, err := d.readBytes()
			if err != nil {
				return err
			}
			largest, err := d.readBytes()
			if err != nil {
				return err
			}
			v.newFiles = append(v.newFiles, newFileEntry{
				level:   level,
				subPart: int(subPart),
				meta: fileMetadata{
					fileNum:  fileNum,
					size:     size,
					smallest: smallest,
					largest:  largest,
				},
			})

		case tagPrevLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.prelogNumber = n

		default:
			return errCorruptManifest
		}
	}
	return nil
}

func (v *VersionEdit) encode(w io.Writer) error {
	e := versionEditEncoder{new(bytes.Buffer)}
	if v.comparatorName != "" {
		e.writeUvarint(tagComparator)
		e.writeString(v.comparatorName)
	}
	if v.logNumber != 0 {
		e.writeUvarint(tagLogNumber)
		e.writeUvarint(v.logNumber)
	}
	if v.prelogNumber != 0 {
		e.writeUvarint(tagPrevLogNumber)
		e.writeUvarint(v.prelogNumber)
	}
	if v.nextFileNumber != 0 {
		e.writeUvarint(tagNextFileNumber)
		e.writeUvarint(v.nextFileNumber)
	}
	if v.lastSequence != 0 {
		e.writeUvarint(tagLastSequence)
		e.writeUvarint(v.lastSequence)
	}
	if v.partNum != 0 {
		e.writeUvarint(tagPartNum)
		e.writeUvarint(uint64(v.partNum))
	}
	for _, x := range v.compactPointers {
		e.writeUvarint(tagCompactPointer)
		e.writeUvarint(uint64(x.level))
		e.writeBytes(x.key)
	}
	for x := range v.deletedFiles {
		e.writeUvarint(tagDeletedFile)
		e.writeUvarint(uint64(x.level))
		e.writeUvarint(uint64(x.subPart))
		e.writeUvarint(x.fileNum)
		e.writeUvarint(uint64(x.logicDelete))
	}
	for _, x := range v.newFiles {
		e.writeUvarint(tagNewFile)
		e.writeUvarint(uint64(x.level))
		e.writeUvarint(uint64(x.subPart))
		e.writeUvarint(x.meta.fileNum)
		e.writeUvarint(x.meta.size)
		e.writeBytes(*(*[]byte)(unsafe.Pointer(&x.meta.smallest)))
		e.writeBytes(*(*[]byte)(unsafe.Pointer(&x.meta.largest)))
	}
	_, err := w.Write(e.Bytes())
	return err
}

type versionEditDecoder struct {
	byteReader
}

func (d versionEditDecoder) readBytes() ([]byte, error) {
	n, err := d.readUvarint()
	if err != nil {
		return nil, err
	}
	s := make([]byte, n)
	_, err = io.ReadFull(d, s)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, errCorruptManifest
		}
		return nil, err
	}
	return s, nil
}

func (d versionEditDecoder) readLevel() (int, error) {
	u, err := d.readUvarint()
	if err != nil {
		return 0, err
	}
	if u >= numLevels {
		return 0, errCorruptManifest
	}
	return int(u), nil
}

func (d versionEditDecoder) readUvarint() (uint64, error) {
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF {
			return 0, errCorruptManifest
		}
		return 0, err
	}
	return u, nil
}

type versionEditEncoder struct {
	*bytes.Buffer
}

func (e versionEditEncoder) writeBytes(p []byte) {
	e.writeUvarint(uint64(len(p)))
	e.Write(p)
}

func (e versionEditEncoder) writeString(s string) {
	e.writeUvarint(uint64(len(s)))
	e.WriteString(s)
}

func (e versionEditEncoder) writeUvarint(u uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	e.Write(buf[:n])
}

type bulkVersionEdit struct {
	partNum       int
	added         [numLevels][][]fileMetadata
	deleted       [numLevels][]map[uint64]bool
	phsicalDelete []uint64
}

func (b *bulkVersionEdit) accumulate(ve *VersionEdit) {
	//解决压缩点
	// for _, cp := range ve.compactPointers {
	// 	_ = cp
	// }
	if ve.partNum^b.partNum != 0 && ve.partNum^b.partNum != ve.partNum {
		log.Fatalf("bulkVersionEdit的partNum不对\r\n")
		return
	}
	b.partNum = ve.partNum
	for df := range ve.deletedFiles {
		dmap := b.deleted[df.level] //df是map的key，也就是deletdEntry
		if len(dmap) == 0 {

			dmap = make([]map[uint64]bool, ve.partNum)
			for i := 0; i < ve.partNum; i++ {
				dmap[i] = make(map[uint64]bool)
			}
			b.deleted[df.level] = dmap

		}
		if df.logicDelete != 1 {
			b.phsicalDelete = append(b.phsicalDelete, df.fileNum)
		}
		dmap[df.subPart][df.fileNum] = true
	}

	for _, nf := range ve.newFiles {
		if dmap := b.deleted[nf.level]; dmap != nil {
			delete(dmap[nf.subPart], nf.meta.fileNum)
		}
		if len(b.added[nf.level]) == 0 {
			b.added[nf.level] = make([][]fileMetadata, ve.partNum)
			for i := 0; i < ve.partNum; i++ {
				b.added[nf.level][i] = make([]fileMetadata, 0, 2)
			}
		}
		b.added[nf.level][nf.subPart] = append(b.added[nf.level][nf.subPart], nf.meta)

	}

}

func (b *bulkVersionEdit) apply(base *Version, icmp internal.SeparatorByteComparer) (*Version, error) {
	//新建banben

	v := new(Version)

	for i := 0; i < len(v.files); i++ {
		v.files[i] = make([][]fileMetadata, b.partNum)
	}
	//对于新版本的每一层，由base的本层的level的filemeta和第二层新增的filemeta，两层加一块去掉删除的组成
	for level := range v.files {
		combined := [2][][]fileMetadata{nil, b.added[level]}
		if base != nil {
			combined[0] = base.files[level]
		}

		for _, ff := range combined {
			for p, part := range ff {
				dmap := b.deleted[level]
				for _, f := range part {
					n := len(ff[p]) + len(ff[p])
					if n == 0 {
						continue
					}
					//v.files[level][p] = make([]fileMetadata, 0, n)
					if dmap != nil {
						t, ok := dmap[p][f.fileNum]
						if ok && t {

							continue
						}
					}

					v.files[level][p] = append(v.files[level][p], f)
				}
				if len(part) > 0 {
					//log.Printf("%d,%d\r\n", level, p)
					sort.Sort(bySmallest{v.files[level][p], icmp})
				}

			}

		}

	}
	v.deletes = b.phsicalDelete
	b.phsicalDelete = nil
	// if err := v.checkOrdering(icmp); err != nil {
	// 	return nil, fmt.Errorf("leveldb: internal error: %v", err)
	// }
	// v.updateCompactionScore()
	return v, nil

}
