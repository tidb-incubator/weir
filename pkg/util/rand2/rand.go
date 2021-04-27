package rand2

import (
	"math/rand"
	"sync"
)

type Rand struct {
	sync.Mutex
	stdRand *rand.Rand
}

func New(src rand.Source) *Rand {
	return &Rand{
		stdRand: rand.New(src),
	}
}

func (r *Rand) Int63() int64 {
	r.Lock()
	ret := r.stdRand.Int63()
	r.Unlock()
	return ret
}

func (r *Rand) Uint32() uint32 {
	r.Lock()
	ret := r.stdRand.Uint32()
	r.Unlock()
	return ret

}

func (r *Rand) Uint64() uint64 {
	r.Lock()
	ret := r.stdRand.Uint64()
	r.Unlock()
	return ret
}

func (r *Rand) Int31() int32 {
	r.Lock()
	ret := r.stdRand.Int31()
	r.Unlock()
	return ret
}

func (r *Rand) Int() int {
	r.Lock()
	ret := r.stdRand.Int()
	r.Unlock()
	return ret
}

func (r *Rand) Int63n(n int64) int64 {
	r.Lock()
	ret := r.stdRand.Int63n(n)
	r.Unlock()
	return ret
}

func (r *Rand) Int31n(n int32) int32 {
	r.Lock()
	ret := r.stdRand.Int31n(n)
	r.Unlock()
	return ret
}

func (r *Rand) Intn(n int) int {
	r.Lock()
	ret := r.stdRand.Intn(n)
	r.Unlock()
	return ret
}

func (r *Rand) Float64() float64 {
	r.Lock()
	ret := r.stdRand.Float64()
	r.Unlock()
	return ret
}

func (r *Rand) Float32() float32 {
	r.Lock()
	ret := r.stdRand.Float32()
	r.Unlock()
	return ret
}
