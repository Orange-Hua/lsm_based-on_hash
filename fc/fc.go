package fc

import (
	"kv/internal"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type FlowCtl struct {
	GetFromDB    chan *internal.Chunk
	Db2table     chan *internal.Chunk
	GetFromTable chan *internal.Chunk
	VictimToDisk chan *internal.Chunk
	Notify       chan Msg
	quick        bool
	tableSpeed   int
	dbSpeed      int
	StatisticPos atomic.Int32
	minDBSpeed   int
	midDBSpeed   int
	curPos       int
	maxDBSpeed   int
	iniDBSpeed   int

	bufferGetFromDB                   chan *internal.Chunk
	bufferGetFromTable                chan *internal.Chunk
	currTableNum                      int
	currDBNum                         int
	lowLimit, generalLimit, highLimit int

	Ctling atomic.Bool
	Signal chan struct{}
	DBLock sync.Mutex
	Cond   *sync.Cond
}
type Msg struct {
	Code uint8
	Typ  uint8
	Pos  uint8
}

func NewFlowCtl(N int, lowLimit, generalLimit, highLimit int) *FlowCtl {
	d1, d2, t1, t2 := make(chan *internal.Chunk, N), make(chan *internal.Chunk, N), make(chan *internal.Chunk, N), make(chan *internal.Chunk, N)
	fc := &FlowCtl{GetFromDB: d1, Db2table: d2, GetFromTable: t1, VictimToDisk: t2}
	fc.minDBSpeed = 1024
	fc.midDBSpeed = 327680
	fc.maxDBSpeed = int(math.MaxInt32)
	fc.iniDBSpeed = 40960
	fc.dbSpeed = fc.iniDBSpeed
	fc.lowLimit = lowLimit
	fc.generalLimit = generalLimit
	fc.highLimit = highLimit
	fc.Notify = make(chan Msg, highLimit*2)
	fc.curPos = 0
	fc.Cond = &sync.Cond{L: &fc.DBLock}
	fc.Signal = make(chan struct{}, 1)
	go fc.run()
	return fc
}

func (fc *FlowCtl) adjustDBSpeed(pos int, currSpeed int) {
	if currSpeed <= 0 && fc.tableSpeed > 0 {
		return
	}
	if currSpeed > 0 {
		if pos <= fc.lowLimit && currSpeed == fc.dbSpeed {

			fc.dbSpeed = fc.dbSpeed << 1
			//log.Println("限速了增加一倍")
		} else if pos > fc.generalLimit && pos < fc.highLimit {

			fc.dbSpeed = fc.dbSpeed - fc.dbSpeed>>1
			//log.Println("限速了减少一半")
		} else if pos >= fc.highLimit {
			fc.dbSpeed = fc.minDBSpeed
			//log.Println("限速了为0")
		}
	}

	if fc.dbSpeed <= 0 && pos >= 0 && pos < fc.highLimit {
		//log.Println("速度恢复")
		if pos <= fc.lowLimit {
			fc.dbSpeed = fc.iniDBSpeed
		} else if pos > fc.lowLimit && pos <= fc.generalLimit {
			fc.dbSpeed = fc.midDBSpeed
		} else if pos > fc.generalLimit && pos < fc.highLimit {
			fc.dbSpeed = fc.minDBSpeed
		}
	}
	if fc.dbSpeed > fc.maxDBSpeed {
		fc.dbSpeed = fc.maxDBSpeed
	}
	if fc.dbSpeed < fc.minDBSpeed {
		fc.dbSpeed = fc.dbSpeed
	}
	//log.Printf("db pos是：%d条\r\n", pos)
}

func (fc *FlowCtl) run() {
	t := time.NewTimer(500 * time.Millisecond)
	for {

		select {
		case <-t.C:
			//log.Printf("一秒内速度实时速度是%d,实时位置是%d,流控速度是%d\r\n", fc.currDBNum, fc.curPos, fc.dbSpeed)
			curPos := fc.StatisticPos.Load()
			fc.adjustDBSpeed(int(curPos), fc.currDBNum)
			fc.currDBNum = 0

			if fc.dbSpeed > 0 {

				if fc.GetFromDB == nil {

					fc.GetFromDB = fc.bufferGetFromDB
					fc.bufferGetFromDB = nil
					fc.Ctling.Store(false)
					select {
					case fc.Signal <- struct{}{}:
					default:
					}

					//fc.Cond.Signal()
				}

			}

			t = time.NewTimer(500 * time.Millisecond)

			// if fc.GetFromTable == nil {
			// 	fc.GetFromTable = fc.bufferGetFromTable
			// 	fc.bufferGetFromTable = nil
			// }

		case c := <-fc.GetFromDB:
			fc.Db2table <- c
			//log.Println("经过流控")
			fc.currDBNum += len(c.Data)
			if fc.currDBNum >= fc.dbSpeed {
				fc.Ctling.Store(true)
				fc.bufferGetFromDB = fc.GetFromDB
				fc.GetFromDB = nil
			}

			// fc.currTableNum++
			// if fc.currTableNum >= fc.tableSpeed {
			// 	fc.bufferGetFromTable = fc.GetFromTable
			// 	fc.GetFromTable = nil
			// }

		}
	}
}
func max(x int, y int) int {

	if x <= y {
		return y
	}
	if x > y {
		return x
	}
	return -1
}
func min(x int, y int) int {

	if x <= y {
		return x
	}
	if x > y {
		return y
	}
	return -1
}
