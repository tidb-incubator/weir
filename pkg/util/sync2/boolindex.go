package sync2

import "sync/atomic"

// BoolIndex rolled array switch mark
type BoolIndex struct {
	index int32
}

// Set set index value
func (b *BoolIndex) Set(index bool) {
	if index {
		atomic.StoreInt32(&b.index, 1)
	} else {
		atomic.StoreInt32(&b.index, 0)
	}
}

// Get return current, next, current bool value
func (b *BoolIndex) Get() (int32, int32, bool) {
	index := atomic.LoadInt32(&b.index)
	if index == 1 {
		return 1, 0, true
	}
	return 0, 1, false
}
