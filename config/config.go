package config

type Config struct {
}

type Options struct {
	Sopt          StorageOpt
	Copt          CacheOpt
	Topt          TableOpt
	NumCounters   int64
	LowHotItemNum int
	Interval      [][2]uint64
	LowLimit      int
	GeneralLimit  int
	HighLimit     int
}

type CacheOpt struct {
	MaxCapacity uint64
}

type TableOpt struct {
	MaxLevel      int
	MaxCapacity   uint64
	Interval      [][2]uint64
	ConcurDiskNum int
	ImmuTableNum  int
}

type StorageOpt struct {
	TableCacheSize         int
	VerifyChecksums        bool
	UseFilterPolicy        bool
	SubPartNum             int
	ConcurCompactionNum    int
	OnelevelCompactionSize int
	ZeroCompactionTrigger  int
	ZeroMaxWritesTrigger   int
	ImmuTableNum           int
	Dir                    string
	MaxBufferNum           int
}
