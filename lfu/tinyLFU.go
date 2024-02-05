package lfu

import (
	"log"
	"sync"

	"github.com/dgraph-io/ristretto/z"
)

type TinyLFU struct {
	freq    *cmSketch
	door    *z.Bloom
	incrs   int64
	resetAt int64
	mutex   sync.RWMutex
}

func NewTinyLFU(numCounters int64) *TinyLFU {
	return &TinyLFU{
		freq:    newCmSketch(numCounters),
		door:    z.NewBloomFilter(float64(numCounters), 0.01),
		resetAt: numCounters,
	}
}

func (p *TinyLFU) Push(keys []uint64) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, key := range keys {
		p.Increment(key)
	}
	log.Println("kv/lfu/tinyLfu.go-Push():计算完了分数")
	return true
}

func (p *TinyLFU) Estimate(key uint64) int64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	hits := p.freq.Estimate(key)
	if p.door.Has(key) {
		hits++
	}
	return hits
}

func (p *TinyLFU) Increment(key uint64) {
	// Flip doorkeeper bit if not already done.
	if added := p.door.AddIfNotHas(key); !added {
		// Increment count-min counter if doorkeeper bit is already set.
		p.freq.Increment(key)
	}
	p.incrs++
	if p.incrs >= p.resetAt {
		p.reset()
	}
}

func (p *TinyLFU) reset() {
	// Zero out incrs.
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.incrs = 0
	// clears doorkeeper bits
	p.door.Clear()
	// halves count-min counters
	p.freq.Reset()
}

func (p *TinyLFU) clear() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.incrs = 0
	p.door.Clear()
	p.freq.Clear()
}
