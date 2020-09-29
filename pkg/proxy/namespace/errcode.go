package namespace

import (
	"github.com/pingcap/errors"
)

var (
	ErrDuplicatedUser      = errors.New("duplicated user")
	ErrInvalidSelectorType = errors.New("invalid selector type")
	ErrNamespaceNotFound   = errors.New("namespace not found")
	ErrDuplicatedNamespace = errors.New("duplicated namespace")
	ErrInitBackend         = errors.New("init backend error")
	ErrInitFrontend        = errors.New("init frontend error")
	ErrInitUsers           = errors.New("init users error")
)
