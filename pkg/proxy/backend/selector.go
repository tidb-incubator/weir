package backend

import (
	"errors"
	"math/rand"
)

var (
	ErrNoInstanceToSelect = errors.New("no instance to select")
)

type Selector interface {
	Select(instances []*Instance) (*Instance, error)
}

type RandomSelector struct {
	rd *rand.Rand
}

func NewRandomSelector(rd *rand.Rand) *RandomSelector {
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
