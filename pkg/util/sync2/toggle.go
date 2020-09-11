package sync2

import (
	"errors"
	"sync"
)

var (
	ErrToggleNotPrepared = errors.New("not prepared")
)

type Toggle struct {
	data     [2]interface{}
	idx      int32
	prepared bool
	lock     sync.RWMutex
}

func NewToggle(o interface{}) *Toggle {
	return &Toggle{
		data: [2]interface{}{o},
	}
}

func (t *Toggle) Current() interface{} {
	t.lock.RLock()
	ret := t.data[t.idx]
	t.lock.RUnlock()
	return ret
}

func (t *Toggle) SwapOther(o interface{}) interface{} {
	t.lock.Lock()
	defer t.lock.Unlock()

	tidx := toggleIdx(t.idx)
	origin := t.data[tidx]
	t.data[tidx] = o
	t.prepared = true
	return origin
}

func (t *Toggle) Toggle() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	currIdx := t.idx
	if !t.prepared {
		return ErrToggleNotPrepared
	}

	t.idx = toggleIdx(currIdx)
	t.prepared = false
	return nil
}

func toggleIdx(idx int32) int32 {
	return (idx + 1) % 2
}
