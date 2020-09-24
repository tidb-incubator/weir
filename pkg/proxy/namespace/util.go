package namespace

import (
	"sync"
	"time"

	"github.com/pingcap-incubator/weir/pkg/util/sync2"
)

type ToggleMapWrapper struct {
	sync.RWMutex
	m map[string]*sync2.Toggle

	delayCloseDuration time.Duration
	handleClose        func(interface{})
}

func NewToggleMapWrapperWithoutCloseFunc(values map[string]interface{}) *ToggleMapWrapper {
	return NewToggleMapWrapper(values, 0, func(interface{}) {})
}

func NewToggleMapWrapper(values map[string]interface{}, delay time.Duration, closeFunc func(value interface{})) *ToggleMapWrapper {
	w := &ToggleMapWrapper{
		m:                  make(map[string]*sync2.Toggle),
		delayCloseDuration: delay,
		handleClose:        closeFunc,
	}
	for k, v := range values {
		w.m[k] = sync2.NewToggle(v)
	}
	return w
}

func (t *ToggleMapWrapper) Get(name string) (interface{}, bool) {
	t.RLock()
	v, ok := t.m[name]
	t.RUnlock()
	return v.Current(), ok
}

func (t *ToggleMapWrapper) ReloadPrepare(name string, value interface{}) error {
	t.RLock()
	defer t.RUnlock()

	v, ok := t.m[name]
	if !ok {
		return ErrNamespaceNotFound
	}

	originValue := v.SwapOther(value)
	t.handleClose(originValue)
	return nil
}

func (t *ToggleMapWrapper) ReloadCommit(name string) error {
	t.RLock()
	defer t.RUnlock()

	v, ok := t.m[name]
	if !ok {
		return ErrNamespaceNotFound
	}

	if err := v.Toggle(); err != nil {
		return err
	}

	originValue := v.SwapOther(nil)
	go func(valueToClose interface{}) {
		if int64(t.delayCloseDuration) > 0 {
			time.Sleep(t.delayCloseDuration)
		}
		t.handleClose(valueToClose)
	}(originValue)

	return nil
}

func (t *ToggleMapWrapper) Add(name string, value interface{}) error {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.m[name]; ok {
		return ErrDuplicatedNamespace
	}

	v := sync2.NewToggle(value)
	t.m[name] = v
	return nil
}

func (t *ToggleMapWrapper) Remove(name string) error {
	t.Lock()
	defer t.Unlock()

	v, ok := t.m[name]
	if !ok {
		return ErrNamespaceNotFound
	}

	t.handleClose(v.Current())
	delete(t.m, name)
	return nil
}

func (t *ToggleMapWrapper) Close() {
	t.Lock()
	defer t.Unlock()

	for _, v := range t.m {
		t.handleClose(v.Current())
	}
}
