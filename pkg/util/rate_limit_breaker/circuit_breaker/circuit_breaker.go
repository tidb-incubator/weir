package circuit_breaker

import (
	"context"
	"errors"
	. "github.com/tidb-incubator/weir/pkg/util/rate_limit_breaker"
	"sync"
)

const (
	CircuitBreakerStatusClosed    = int32(0)
	CircuitBreakerStatusOpen      = int32(1)
	CircuitBreakerStatusHalfOpen  = int32(2)
	CircuitBreakerStatusForceOpen = int32(3)
)

type CircuitBreakerConfig struct {
	minQPS               int64
	failureRateThreshold int64 // 错误率(百分比)
	OpenStatusDurationMs int64
	forceOpen            bool
	failureNum           int64
	size                 int64
	cellIntervalMs       int64
}

type CircuitBreaker struct {
	mu *sync.Mutex    // guard sw, config, status, halfOpenStartMs, halfOpenProbeSent
	sw *SlidingWindow // guarded by mu
	// 熔断器配置，仅通过 ChangeConfig() 方法修改
	config CircuitBreakerConfig // guarded by mu
	// 熔断器状态，随运行状态而随时变化
	status            int32 // guarded by mu
	openStartMs       int64 // guarded by mu
	halfOpenStartMs   int64 // guarded by mu
	halfOpenProbeSent bool  // guarded by mu
}

func NewCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{}
}

func (this *CircuitBreakerConfig) SetMinQPS(minQPS int64) *CircuitBreakerConfig {
	this.minQPS = minQPS
	return this
}

func (this *CircuitBreakerConfig) SetFailureRateThreshold(failureRateThreshold int64) *CircuitBreakerConfig {
	this.failureRateThreshold = failureRateThreshold
	return this
}

func (this *CircuitBreakerConfig) SetFailureNum(failureNum int64) *CircuitBreakerConfig {
	this.failureNum = failureNum
	return this
}

func (this *CircuitBreakerConfig) SetOpenStatusDurationMs(OpenStatusDurationMs int64) *CircuitBreakerConfig {
	this.OpenStatusDurationMs = OpenStatusDurationMs
	return this
}

func (this *CircuitBreakerConfig) SetForceOpen(forceOpen bool) *CircuitBreakerConfig {
	this.forceOpen = forceOpen
	return this
}

func (this *CircuitBreakerConfig) SetSize(size int64) *CircuitBreakerConfig {
	this.size = size
	return this
}

func (this *CircuitBreakerConfig) SetCellIntervalMs(cellIntervalMs int64) *CircuitBreakerConfig {
	this.cellIntervalMs = cellIntervalMs
	return this
}

func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	status := CircuitBreakerStatusClosed // 默认为 closed 状态
	if config.forceOpen {
		status = CircuitBreakerStatusForceOpen
	}
	return &CircuitBreaker{
		mu: &sync.Mutex{},
		sw: NewSlidingWindow(config.size, config.cellIntervalMs), // 10 个窗口，各 1s，共 10s。
		config: CircuitBreakerConfig{
			minQPS:               config.minQPS,
			failureRateThreshold: config.failureRateThreshold,
			failureNum:           config.failureNum,
			OpenStatusDurationMs: config.OpenStatusDurationMs,
			forceOpen:            config.forceOpen,
			size:                 config.size,
			cellIntervalMs:       config.cellIntervalMs,
		},
		status:            status,
		openStartMs:       0,
		halfOpenStartMs:   0,
		halfOpenProbeSent: false,
	}
}

func (cb *CircuitBreaker) ChangeConfig(config *CircuitBreakerConfig) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	oldConfig := cb.config
	cb.config = CircuitBreakerConfig{
		minQPS:               config.minQPS,
		failureRateThreshold: config.failureRateThreshold,
		OpenStatusDurationMs: config.OpenStatusDurationMs,
		failureNum:           config.failureNum,
		forceOpen:            config.forceOpen,
		size:                 config.size,
		cellIntervalMs:       config.cellIntervalMs,
	}

	if config.forceOpen {
		cb.status = CircuitBreakerStatusForceOpen
		cb.openStartMs = 0
		cb.halfOpenStartMs = 0
		cb.halfOpenProbeSent = false
		for _, cell := range cb.sw.Cells {
			cell.Reset()
		}
	} else {
		if cb.status == CircuitBreakerStatusForceOpen {
			// 当 forceOpen 从打开变为关闭时，回到 closed 状态。
			cb.status = CircuitBreakerStatusClosed
		}

		if cb.status == CircuitBreakerStatusOpen && config.minQPS > oldConfig.minQPS {
			// 如果提高了 minQPS，而当前已为熔断状态，则置为 closed 状态。
			cb.status = CircuitBreakerStatusClosed
		}
	}
}

func (cb *CircuitBreaker) GetHalfOpenProbeSent() bool {
	return cb.halfOpenProbeSent
}

func (cb *CircuitBreaker) SetHalfOpenProbeSent(halfOpenProbeSent bool) {
	cb.halfOpenProbeSent = halfOpenProbeSent
}

func (cb *CircuitBreaker) Status() int32 {
	// 从 closed -> open 是根据错误率的变化来触发的(Hit 方法)
	// 从 open -> half_open 是根据时间来触发(此方法)。
	// 从 half_open -> open, 和 half_open -> close 是根据 probe 来触发的(也是通过 Hit 方法)
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.config.forceOpen == true {
		return CircuitBreakerStatusForceOpen
	}

	status := cb.status
	nowMs := GetNowMs()
	if status == CircuitBreakerStatusOpen && nowMs-cb.openStartMs > cb.config.OpenStatusDurationMs {
		cb.status = CircuitBreakerStatusHalfOpen
		cb.halfOpenStartMs = cb.openStartMs + cb.config.OpenStatusDurationMs
		cb.halfOpenProbeSent = false
		// 重置其他状态字段
		cb.openStartMs = 0
		status = CircuitBreakerStatusHalfOpen
	}
	return status
}

var ErrCircuitBreak error = errors.New("circuit breaker triggered")

type runFunc func(context.Context) error
type fallbackFunc func(context.Context, error) error

func (cb *CircuitBreaker) Do(ctx context.Context, run runFunc, fallback fallbackFunc) error {
	if fallback == nil {
		fallback = func(ctx context.Context, err error) error {
			return err
		}
	}

	status := cb.Status()
	switch status {
	case CircuitBreakerStatusClosed:
		failed := false
		err := run(ctx)
		if err != nil {
			failed = true
		}
		cb.Hit(GetNowMs(), false, failed)
		if failed {
			return fallback(ctx, err)
		}
	case CircuitBreakerStatusOpen:
		return fallback(ctx, ErrCircuitBreak)
	case CircuitBreakerStatusHalfOpen:
		cb.mu.Lock()
		halfOpenProbeSent := cb.halfOpenProbeSent
		if !cb.halfOpenProbeSent {
			cb.halfOpenProbeSent = true
		}
		cb.mu.Unlock()

		if halfOpenProbeSent {
			return fallback(ctx, ErrCircuitBreak)
		}

		// send probe
		failed := false
		err := run(ctx)
		if err != nil {
			failed = true
		}
		cb.Hit(GetNowMs(), true, failed)
		if failed {
			return fallback(ctx, err)
		}
	case CircuitBreakerStatusForceOpen:
		return fallback(ctx, ErrCircuitBreak)
	}
	return nil
}

const (
	TotalHit   = "total"
	FailureHit = "failure"
)

// 根据请求的成败，驱动 CircuitBreaker 状态迁移。
func (cb *CircuitBreaker) Hit(nowMs int64, isProbe bool, isFailureHit bool) {
	status := cb.Status()
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch status {
	case CircuitBreakerStatusClosed:
		metrics := []string{TotalHit}
		if isFailureHit {
			metrics = []string{TotalHit, FailureHit}
		}
		cb.sw.Hit(nowMs, metrics...)

		if isFailureHit {
			statusCouldChange := false

			if cb.config.failureNum != 0 {
				nowHitsStat := cb.sw.GetNowHits(nowMs, TotalHit, FailureHit)
				failureHits := nowHitsStat[FailureHit]
				totalHits := nowHitsStat[TotalHit]
				if failureHits > cb.config.failureNum && (totalHits > cb.config.minQPS) {
					statusCouldChange = true
				}
			} else {
				hitsStat := cb.sw.GetHits(nowMs, TotalHit, FailureHit)
				failureHits := hitsStat[FailureHit]
				totalHits := hitsStat[TotalHit]
				failureRate := int64(float64(failureHits) * 100 / float64(totalHits))
				if failureRate > cb.config.failureRateThreshold && (totalHits*1000/(cb.sw.Size*cb.sw.CellIntervalMs) > cb.config.minQPS) {
					statusCouldChange = true
				}
			}

			if statusCouldChange {
				cb.status = CircuitBreakerStatusOpen
				cb.openStartMs = nowMs
				// reset other fields
				cb.halfOpenStartMs = 0
				cb.halfOpenProbeSent = false
			}
		}
	case CircuitBreakerStatusOpen:
		// 出现此种情况，因为发起调用时状态为 closed，但调用完成时状态已经变为 open 了。
		// pass
	case CircuitBreakerStatusHalfOpen:
		if !isProbe {
			// 出现这种情况，因为发起调用时状态为 closed，但此调用非常之慢，等到调用完成，状态已经由 open 而又变成 halfOpen 了。
			return
		}

		if isFailureHit {
			cb.status = CircuitBreakerStatusOpen
			cb.openStartMs = nowMs
			// reset other fields
			cb.halfOpenStartMs = 0
			cb.halfOpenProbeSent = false
		} else {
			cb.status = CircuitBreakerStatusClosed
			cb.halfOpenStartMs = nowMs
			cb.halfOpenProbeSent = false
			// reset other fields
			cb.openStartMs = nowMs
		}
	case CircuitBreakerStatusForceOpen:
		// 出现这种情况，因为发起调用时状态为 closed，但等到调用完成时，circuit breaker 已被重置为 force open 了。
	}
}
