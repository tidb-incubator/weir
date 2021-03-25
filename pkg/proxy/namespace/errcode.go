package namespace

import (
	"github.com/pingcap/errors"
)

var (
	ErrDuplicatedUser      = errors.New("duplicated user")
	ErrInvalidSelectorType = errors.New("invalid selector type")

	ErrNilBreakerName              = errors.New("breaker name nil")
	ErrInvalidFailureRateThreshold = errors.New("invalid FailureRateThreshold")
	ErrInvalidopenStatusDurationMs = errors.New("invalid OpenStatusDurationMs")
	ErrInvalidSqlTimeout           = errors.New("invalid sql timeout")

	ErrInvalidScope = errors.New("invalid scope")
)
