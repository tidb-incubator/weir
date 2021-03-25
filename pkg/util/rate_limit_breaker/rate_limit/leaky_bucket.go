package rate_limit

import (
	"sync/atomic"
	"time"
)

/* 并发的 rate limiter。
 * 基于队列的 leaky bucket 算法实现。
 * 参见 https://en.wikipedia.org/wiki/Leaky_bucket leaky bucket 有两种实现方式
 * As a meter: 此与 token bucket 等价
 * As a queue: 此具有更严格的限速，能够避免 burst flow
 *
 *
 * 测试结果(MacBook Pro 16 英寸):
 *  1K  QPS: 1 req per   1 milli sec (   1ms)
 *  1w  QPS: 1 req per 100 micro sec ( 0.1ms)  timer 可以支持此精度。
 *  2w  QPS: 1 req per  50 micro sec (0.05ms)  timer 可以支持此精度。
 * 10w  QPS: 1 req per  10 micro sec (0.01ms)  timer 开始出现误差。
 * 为确保精度，建议不要超过 10w QPS。
 * (此问题并非无解，是有优化方案的。)
 */
type LeakyBucketRateLimiter struct {
	qpsThreshold int64              // 共享，需通过原子操作进行读写
	ch           chan chan struct{} // the leaky bucket
	stopCh       chan struct{}      // 用于关闭这个 rate limiter
	changeCh     chan struct{}      // 修改本 rate limiter 之后，通过此 channel 进行通知
}

func NewLeakyBucketRateLimiter(qpsThreshold int64) *LeakyBucketRateLimiter {
	lbrl := &LeakyBucketRateLimiter{
		qpsThreshold: qpsThreshold,
		ch:           make(chan chan struct{}, 1),
		stopCh:       make(chan struct{}),
		changeCh:     make(chan struct{}),
	}
	go func() {
		lbrl.leak()
	}()
	return lbrl
}

func (lbrl *LeakyBucketRateLimiter) ChangeQpsThreshold(newQpsThreshold int64) {
	atomic.StoreInt64(&lbrl.qpsThreshold, newQpsThreshold)
	lbrl.changeCh <- struct{}{}
}

func (lbrl *LeakyBucketRateLimiter) getTick() time.Duration {
	qpsThreshold := atomic.LoadInt64(&lbrl.qpsThreshold)
	tick := time.Duration(1000.0 * int64(time.Millisecond) / qpsThreshold)
	return tick
}

func (lbrl *LeakyBucketRateLimiter) leak() {
	// 这里的基本逻辑是，根据 qpsThreshold 计算 tick，每个 tick 的时间间隔里，只允许一次动作。
	// 然而，qpsThreshold 很大时，tick 很小。这带来两个问题：
	// 1) timer 的精度可能无法满足要求，导致误差变大
	// 2) 本方法循环次数很多，占用较多 CPU
	// 一个优化方案，是去掉「每个 tick 的时间间隔里，只允许一次动作」限制。
	// 而是首先确保 tick 保持在合理的范围(如 >=0.1ms 且越小越好)，并根据该 tick 和 qpsThreshold 确定每个 tick 里允许的动作次数(须为整数)。
	tickCh := time.Tick(lbrl.getTick())
OUTER:
	for {
		select {
		case <-lbrl.stopCh: // stopped
			break OUTER
		case <-lbrl.changeCh: // rate limiter modified
			newTick := lbrl.getTick()
			tickCh = time.Tick(newTick)
		case <-tickCh:
			select {
			case waiterCh := <-lbrl.ch:
				waiterCh <- struct{}{}
			default:
				// pass
			}
		}
	}
}

func (lbrl *LeakyBucketRateLimiter) Limit() error {
	ch := make(chan struct{}, 1)
	lbrl.ch <- ch
	<-ch
	return nil
}

func (lbrl *LeakyBucketRateLimiter) Close() {
	lbrl.stopCh <- struct{}{}
}
