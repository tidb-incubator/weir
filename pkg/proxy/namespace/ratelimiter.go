package namespace

import (
	"context"
	"sync"

	"github.com/pingcap-incubator/weir/pkg/util/rate_limit_breaker/rate_limit"
)

type NamespaceRateLimiter struct {
	limiters     *sync.Map
	qpsThreshold int
	scope        string
}

func NewNamespaceRateLimiter(scope string, qpsThreshold int) *NamespaceRateLimiter {
	return &NamespaceRateLimiter{
		limiters:     &sync.Map{},
		scope:        scope,
		qpsThreshold: qpsThreshold,
	}
}

func (n *NamespaceRateLimiter) Scope() string {
	return n.scope
}

func (n *NamespaceRateLimiter) Limit(ctx context.Context, key string) error {
	if n.qpsThreshold <= 0 {
		return nil
	}
	limiter, _ := n.limiters.LoadOrStore(key, rate_limit.NewSlidingWindowRateLimiter(int64(n.qpsThreshold)))
	return limiter.(*rate_limit.SlidingWindowRateLimiter).Limit()
}
