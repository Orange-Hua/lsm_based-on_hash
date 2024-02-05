package internal

type Part interface {
	Run() error
}
type Interval struct {
	Data [2]uint64
}

type Intervals struct {
	Data [][2]uint64
}

func (i *Interval) GetData() (uint64, uint64) {
	return i.Data[0], i.Data[1]
}

func (i *Intervals) GetIndex(key uint64) (int, bool) {

	n := len(i.Data)
	for j := 0; j < n; j++ {
		if key >= i.Data[j][0] && key <= i.Data[j][1] {
			return j, true

		}
	}
	return -1, false
}

func (i *Intervals) GetIntervalNum() int {
	return len(i.Data)
}
func (i *Intervals) GetData(index int) (uint64, uint64) {
	return i.Data[index][0], i.Data[index][1]
}
