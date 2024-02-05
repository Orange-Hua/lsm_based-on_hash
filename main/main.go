package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	db "kv"
	"kv/config"
	"kv/internal"
	"kv/utils"
	"log"
	"math"
	_ "net/http/pprof"
	"time"

	"os"
	"strings"
	"sync"
)

var k = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I"}

// func init() {
// 	logFile, err := os.OpenFile("e:\\log.txt", os.O_WRONLY|os.O_CREATE, 0660)
// 	if err != nil {
// 		panic("log写文件失败\r\n")
// 	}
// 	log.SetOutput(logFile)

// }

const (
	path  = "f:\\kv\\data\\s1.txt"
	path1 = "e:\\s1.txt"
	path2 = "e:\\s2.txt" //200m
	path3 = "e:\\s3.txt" //2g
	path4 = "e:\\s4.txt"
)

var (
	seq    uint64 = 0
	total         = 10000
	strLen        = 10
	deli          = "  "
	w             = false
)

func ReadRawData() [][]string {
	var p [][]string
	f, err := os.OpenFile(path2, os.O_RDWR, 0660)
	if err != nil {
		log.Fatalf("main ReadKV打开文件失败\r\n")
		return nil
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		s, err := r.ReadString(byte('\n'))
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("main ReadKV文件读取kv失败\r\n")
			return nil
		}
		part := strings.Split(s, deli)
		p = append(p, part)

	}
	return p
}
func ReadKV() []*internal.Pair {
	var p []*internal.Pair
	f, err := os.OpenFile(path4, os.O_RDWR, 0660)
	if err != nil {
		log.Fatalf("main ReadKV打开文件失败\r\n")
		return nil
	}
	defer f.Close()
	r := bufio.NewReader(f)

	for {
		s, err := r.ReadString(byte('\n'))
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("main ReadKV文件读取kv失败\r\n")
			return nil
		}
		part := strings.Split(s, deli)
		key := part[0]
		value := part[1]
		k, _ := utils.KeyToHash(key)
		ik := internal.NewInternalKey(k, 0, seq)
		seq++
		p = append(p, &internal.Pair{Key: ik, Value: []byte(value)})
	}
	return p
}

func WriteKV() {
	f, err := os.OpenFile(path4, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		log.Fatalf("main ReadKV打开文件失败\r\n")
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	n := 0
	for n < total {
		k := utils.RandStr(strLen)
		v := utils.RandStr(strLen)
		line := k + deli + v + "\n"
		w.Write([]byte(line))
		n++
	}
	w.Flush()
}

var wg sync.WaitGroup

type txtIter struct {
	f           *bufio.Reader
	data        *bytes.Buffer
	block       []byte
	line        []byte
	index       int
	soi, eoi    bool
	keyLength   int
	valueLength int
	fLength     int
	err         error
}

func (i *txtIter) Next() bool {
	if i.soi == true && i.eoi == false {
		i.soi = false
		return true
	}
	if i.err != nil {
		return false
	}
	var err error
	i.line, err = i.f.ReadBytes('\n')
	if err != nil {
		i.err = err
		return false
	}
	return true

}

func (i *txtIter) Key() string {
	sb := &strings.Builder{}
	if _, err := sb.Write(i.line[:i.keyLength]); err != nil {
		log.Fatalf("读key出错\r\n")
	}
	return sb.String()
}
func (i *txtIter) Value() []byte {
	temp := make([]byte, 0, i.valueLength)
	temp = append(temp, i.line[i.keyLength+i.fLength:]...)
	return temp
}

func NewTxtIter(keyLength, valueLength, fLength int, reader *bufio.Reader) *txtIter {
	txtIter := txtIter{
		f:           reader,
		keyLength:   keyLength,
		valueLength: valueLength,
		fLength:     fLength,
	}
	txtIter.line, txtIter.err = txtIter.f.ReadBytes('\n')

	txtIter.soi = !txtIter.eoi
	return &txtIter
}

var (
	cacheCapacity         uint64 = 8 * 1024
	tableCapacity         uint64 = 2 * 1024 * 1024
	tableLevel                   = 12
	baseMaxBytes                 = 100 * 1024 * 1024
	conCurCompactionNum          = 4
	conCurDiskNum                = 2
	subPartNum                   = 4
	zeroCompactionTrigger        = 4
	zeroMaxWritesTrigger         = 12
	immuTableNum                 = 8
	dir                          = "e:\\benchmark\\"
	tableCacheSize               = 10000
	// path          = "f:\\kv\\data\\s1.txt"
	// path1         = "e:\\s1.txt"
	// path2         = "e:\\s2.txt" //200m
	// path3         = "e:\\s3.txt" //2g
	rpath = "e:\\s1.txt"
	wpath = "e:\\w.txt"
)

func ReadTXTAndWriteDB() {

	f, err := os.OpenFile(rpath, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return
	}
	defer f.Close()
	wf, err := os.OpenFile(wpath, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return
	}
	defer wf.Close()
	r := bufio.NewReaderSize(f, 10*1024*1024)
	//w := bufio.NewWriter(wf)
	iter := NewTxtIter(10, 10, 2, r)
	opt := config.Options{
		//Interval: [][2]uint64{{0, 9999999999999999999}, {10000000000000000000, math.MaxUint64}},
		Interval:      [][2]uint64{{0, math.MaxUint64}},
		NumCounters:   8,
		LowHotItemNum: 3,
		LowLimit:      4,
		GeneralLimit:  8,
		HighLimit:     12,
		Copt: config.CacheOpt{
			MaxCapacity: cacheCapacity,
		},
		Topt: config.TableOpt{
			MaxLevel:      tableLevel,
			MaxCapacity:   tableCapacity,
			ConcurDiskNum: conCurDiskNum,
			ImmuTableNum:  immuTableNum,
		},
		Sopt: config.StorageOpt{
			TableCacheSize:         tableCacheSize,
			VerifyChecksums:        true,
			UseFilterPolicy:        false,
			SubPartNum:             subPartNum,
			ConcurCompactionNum:    conCurCompactionNum,
			OnelevelCompactionSize: baseMaxBytes,
			ZeroCompactionTrigger:  zeroCompactionTrigger,
			ZeroMaxWritesTrigger:   zeroMaxWritesTrigger,
			MaxBufferNum:           immuTableNum,
			Dir:                    dir,
		},
	}
	db := db.NewDB(&opt)
	//_ = opt
	// options := rosedb.DefaultOptions
	// options.DirPath = "e:\\rosedb"

	// // open a database
	// db, err := rosedb.Open(options)
	// if err != nil {
	// 	panic(err)
	// }
	// defer func() {
	// 	_ = db.Close()
	// }()
	//db, err := leveldb.OpenFile("e:\\rosedb", nil)

	defer db.Close()
	i := 0
	for iter.Next() {

		key := iter.Key()
		value := iter.Value()

		db.Put(key, value, uint64(len(value)))
		//log.Println(i)
		//i++
		//db.Put([]byte(key), value, nil)
		//db.Put(key, value, uint64(len(value)))

	}
	log.Println("写完了")
	f.Seek(0, io.SeekStart)
	iter = NewTxtIter(10, 10, 2, r)

	//n := 0
	i = 0
	s := time.Now().UnixNano()
	for iter.Next() {

		key := iter.Key()
		_, ok := db.Get(key)
		//db.Get([]byte(key), nil)
		//n++
		//i++

		if !ok {
			log.Printf("没找到：%d ,%s\r\n", i, key)

		}
		i++
		//log.Printf("key:%s,value:%s", key, string(v))

		//line := key + deli + string(v)
		//w.Write([]byte(line))

	}
	e := time.Now().UnixNano()
	fmt.Println(e - s)
	//w.Flush()

}

func main() {

	// runtime.SetMutexProfileFraction(1) // 开启对锁调用的跟踪
	// runtime.SetBlockProfileRate(1)     // 开启对阻塞操作的跟踪
	// go func() {

	// 	http.ListenAndServe(":6060", nil)
	// }()

	ReadTXTAndWriteDB()

	// opt := config.Options{
	// 	//Interval: [][2]uint64{{0, 9999999999999999999}, {10000000000000000000, math.MaxUint64}},
	// 	Interval:      [][2]uint64{{0, math.MaxUint64}},
	// 	NumCounters:   8,
	// 	LowHotItemNum: 3,
	// 	Copt: config.CacheOpt{
	// 		MaxCapacity: cacheCapacity,
	// 	},
	// 	Topt: config.TableOpt{
	// 		MaxLevel:      tableLevel,
	// 		MaxCapacity:   tableCapacity,
	// 		ConcurDiskNum: conCurDiskNum,
	// 		ImmuTableNum:  immuTableNum,
	// 	},
	// 	Sopt: config.StorageOpt{
	// 		TableCacheSize:            10000,
	// 		VerifyChecksums:           true,
	// 		UseFilterPolicy:           false,
	// 		SubPartNum:                subPartNum,
	// 		ConcurCompactionNum:       conCurCompactionNum,
	// 		OnelevelCompactionSize:    baseMaxBytes,
	// 		ZeroLevelCompactionLength: zeroCompactLength,
	// 		MaxBufferNum:              immuTableNum,
	// 		Dir:                       dir,
	// 	},
	// }
	// interval := internal.Interval{Data: [2]uint64{0, math.MaxUint64}}
	// s := storage.NewStorage(&interval, "e:\\kvtest\\0", &opt.Sopt)
	// iter, err := s.TC.Find(26, nil, nil)
	// if err != nil {
	// 	fmt.Println("没找到")
	// }
	// for iter.Next() {
	// 	key := iter.Key()
	// 	value := iter.Value()
	// 	k := internal.ByteArrayToInternalKey(key).Key
	// 	if k == 3283396103717156567 {
	// 		fmt.Println(k)
	// 	}

	// 	_ = value
	// }

}

//WriteKV()
