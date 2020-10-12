package namespace

import (
	"github.com/pingcap/errors"
)

var (
	ErrDuplicatedUser      = errors.New("duplicated user")
	ErrInvalidSelectorType = errors.New("invalid selector type")
)
