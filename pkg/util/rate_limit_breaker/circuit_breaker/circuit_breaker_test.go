package circuit_breaker

import (
	"context"
	"errors"
	rateLimitBreaker "github.com/tidb-incubator/weir/pkg/util/rate_limit_breaker"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCircuitBreaker_Do_NoError(t *testing.T) {
	ctx := context.Background()
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		failureNum:           5,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
		size:                 10,
		cellIntervalMs:       1000,
	})

	successCount := 0
	errCount := 0
	for i := 0; i < 1000; i++ {
		cb.Do(ctx, func(ctx context.Context) error {
			successCount++
			return nil
		}, func(ctx context.Context, err error) error {
			errCount++
			return err
		})
	}
	assert.Equal(t, successCount, 1000)
	assert.Equal(t, errCount, 0)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusOpen)
}

func TestCircuitBreaker_Do_AlwaysError(t *testing.T) {
	ctx := context.Background()
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
	})

	successCount := 0
	errCount := 0
	for i := 0; i < 1000; i++ {
		cb.Do(ctx, func(ctx context.Context) error {
			successCount++
			return errors.New("just_error")
		}, func(ctx context.Context, err error) error {
			errCount++
			return err
		})
	}

	assert.True(t, successCount < 1000)
	assert.Equal(t, errCount, 1000)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusOpen)
}

func TestCircuitBreaker_ForceOpen(t *testing.T) {
	ctx := context.Background()
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            true,
	})

	errCount := 0
	for i := 0; i < 1000; i++ {
		cb.Do(ctx, func(ctx context.Context) error {
			return nil
		}, func(ctx context.Context, err error) error {
			errCount++
			return nil
		})
	}
	assert.Equal(t, errCount, 1000)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusForceOpen)
}

func TestCircuitBreaker_ChangeConfig_WithForceOpen(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            true,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusForceOpen)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusForceOpen)

	// cancel forceOpen, status goes back to closed
	cb.ChangeConfig(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000,
		forceOpen:            false,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusClosed)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusClosed)
}

func TestCircuitBreaker_ChangeConfig_WithoutForceOpen(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
	})

	// 当前为 open 状态，然后修改配置(没有强制开启)，检查仍为 open 状态
	cb.status = CircuitBreakerStatusOpen
	cb.openStartMs = rateLimitBreaker.GetNowMs()
	cb.ChangeConfig(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 100000, // 100s
		forceOpen:            false,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusOpen)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusOpen)

	// 当前为 closed 状态，然后修改配置(没有强制开启)，检查仍为 closed 状态
	cb.status = CircuitBreakerStatusClosed
	cb.ChangeConfig(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusClosed)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusClosed)
}
