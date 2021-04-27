package backend

import (
	"errors"
	"math/rand"
	"time"

	"github.com/pingcap-incubator/weir/pkg/util/rand2"
)

const (
	SelectorTypeRandom = 1 + iota
)

const (
	SelectorNameUnknown = "unknown"
	SelectorNameRandom  = "random"
)

var (
	selectorTypeMap = map[int]string{
		SelectorTypeRandom: SelectorNameRandom,
	}
	selectorNameMap = map[string]int{
		SelectorNameRandom: SelectorTypeRandom,
	}
)

var (
	ErrNoInstanceToSelect  = errors.New("no instance to select")
	ErrInvalidSelectorType = errors.New("invalid selector type")
)

type Selector interface {
	Select(instances []*Instance) (*Instance, error)
}

type RandomSelector struct {
	rd *rand2.Rand
}

func CreateSelector(selectorType int) (Selector, error) {
	switch selectorType {
	case SelectorTypeRandom:
		source := rand.NewSource(time.Now().Unix())
		rd := rand2.New(source)
		return NewRandomSelector(rd), nil
	default:
		return nil, ErrInvalidSelectorType
	}
}

func NewRandomSelector(rd *rand2.Rand) *RandomSelector {
	return &RandomSelector{
		rd: rd,
	}
}

func (s *RandomSelector) Select(instances []*Instance) (*Instance, error) {
	length := len(instances)
	if length == 0 {
		return nil, ErrNoInstanceToSelect
	}
	idx := s.rd.Int63n(int64(length))
	return instances[idx], nil
}

func SelectorNameToType(name string) (int, bool) {
	t, ok := selectorNameMap[name]
	return t, ok
}

func SelectorTypeToName(t int) (string, bool) {
	n, ok := selectorTypeMap[t]
	return n, ok
}
