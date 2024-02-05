package main

import (
	db "kv"
	"kv/config"
	"kv/utils"
	"math"
	"math/rand"
	"strconv"
	"testing"
)

/*
200 M 11秒
*/

var opt = config.Options{
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
var db1 = db.NewDB(&opt)

func BenchmarkPut(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		value := utils.RandStr(10)
		db1.Put(utils.RandStr(10), []byte(value), uint64(len(value)))
	}
}

func BenchmarkGet(b *testing.B) {

	for i := 0; i < 1000000; i++ {
		value := utils.RandStr(10)
		db1.Put(strconv.Itoa(i), []byte(value), uint64(len(value)))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {

		db1.Get(strconv.Itoa(rand.Int()))
	}
}
