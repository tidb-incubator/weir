package namespace

import (
	"github.com/pingcap-incubator/weir/pkg/config"
	"github.com/pingcap-incubator/weir/pkg/proxy/driver"
	rb "github.com/pingcap-incubator/weir/pkg/util/rate_limit_breaker"
	cb "github.com/pingcap-incubator/weir/pkg/util/rate_limit_breaker/circuit_breaker"
	"github.com/pingcap-incubator/weir/pkg/util/timer"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type strategyInfo struct {
	minQps               int64
	sqlTimeoutMs         int64
	sqlTimeoutMsDuration time.Duration
	failureRatethreshold int64
	failureNum           int64
	openStatusDurationMs int64
	size                 int64
	cellIntervalMs       int64
}

type BreakerManager struct {
	rwLock     sync.RWMutex
	bmset      map[string]struct{}
	b          []map[string]*cb.CircuitBreaker
	tw         *timer.TimeWheel
	scope      string
	strategies []strategyInfo
	hashFactor uint64
}

type Breaker struct {
	bm *BreakerManager
}

var (
	timeWheelUnit       = time.Millisecond * 10
	timeWheelBucketsNum = 10
)

type StrategySlice []config.StrategyInfo

func (a StrategySlice) Len() int {
	return len(a)
}

func (a StrategySlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a StrategySlice) Less(i, j int) bool {
	return a[j].SqlTimeoutMs > a[i].SqlTimeoutMs
}

func NewBreaker(br *config.BreakerInfo) (*Breaker, error) {
	nbm, err := NewBreakerManager(br)
	if err != nil {
		return nil, err
	}
	return &Breaker{bm: nbm}, nil
}

func (this *Breaker) GetBreaker() (driver.Breaker, error) {
	return this.bm, nil
}

func checkBreakerConfig(s *config.StrategyInfo) error {
	if s.FailureRatethreshold < 0 || s.FailureRatethreshold > 100 {
		return ErrInvalidFailureRateThreshold
	}
	if s.OpenStatusDurationMs <= 0 {
		return ErrInvalidopenStatusDurationMs
	}
	if s.SqlTimeoutMs <= 0 {
		return ErrInvalidSqlTimeout
	}
	return nil
}

func getHashFactor(num int) uint64 {
	HashFactor := 1
	for num > 0 {
		num /= 10
		if num > 0 {
			HashFactor++
		}
	}
	return uint64(HashFactor) * 10
}

func NewBreakerManager(br *config.BreakerInfo) (*BreakerManager, error) {
	strategyLenth := len(br.Strategies)
	strategies := make([]strategyInfo, strategyLenth)
	brArray := make([]map[string]*cb.CircuitBreaker, strategyLenth)

	sort.Sort(StrategySlice(br.Strategies))
	for idx, strategy := range br.Strategies {
		if err := checkBreakerConfig(&strategy); err != nil {
			return nil, err
		}
		strategies[idx] = strategyInfo{
			minQps:               strategy.MinQps,
			sqlTimeoutMs:         strategy.SqlTimeoutMs,
			sqlTimeoutMsDuration: time.Duration(strategy.SqlTimeoutMs) * time.Millisecond,
			failureRatethreshold: strategy.FailureRatethreshold,
			failureNum:           strategy.FailureNum,
			openStatusDurationMs: strategy.OpenStatusDurationMs,
			size:                 strategy.Size,
			cellIntervalMs:       strategy.CellIntervalMs,
		}

		b := make(map[string]*cb.CircuitBreaker)

		brArray[idx] = b
	}

	tw, err := timer.NewTimeWheel(timeWheelUnit, timeWheelBucketsNum)
	if err != nil {
		return nil, err
	}
	tw.Start()

	bmset := make(map[string]struct{})
	return &BreakerManager{
		b:          brArray,
		bmset:      bmset,
		scope:      br.Scope,
		tw:         tw,
		strategies: strategies,
		hashFactor: getHashFactor(strategyLenth),
	}, nil
}

func (this *BreakerManager) IsUseBreaker() bool {
	if len(this.strategies) == 0 {
		return false
	}
	return true
}

func (this *BreakerManager) GetBreakerScope() string {
	return this.scope
}

func (this *BreakerManager) hit(name string, idx int, isFail bool) error {
	nowMs := rb.GetNowMs()
	val, ok := this.b[idx][name]
	if !ok {
		return nil
	}

	switch val.Status() {
	case cb.CircuitBreakerStatusClosed:
		this.b[idx][name].Hit(nowMs, false, isFail)
	case cb.CircuitBreakerStatusOpen:
		return nil
	case cb.CircuitBreakerStatusHalfOpen:
		this.b[idx][name].Hit(nowMs, true, isFail)
	}

	return nil
}

func (this *BreakerManager) Hit(name string, idx int, isFail bool) error {
	this.rwLock.Lock()
	defer this.rwLock.Unlock()
	if _, ok := this.bmset[name]; !ok {
		for idx, strategy := range this.strategies {
			cbc := cb.NewCircuitBreakerConfig().
				SetMinQPS(strategy.minQps).
				SetFailureRateThreshold(strategy.failureRatethreshold).
				SetOpenStatusDurationMs(strategy.openStatusDurationMs).
				SetFailureNum(strategy.failureNum).
				SetSize(strategy.size).
				SetCellIntervalMs(strategy.cellIntervalMs).
				SetForceOpen(false)
			cbObj := cb.NewCircuitBreaker(cbc)
			this.b[idx][name] = cbObj
		}
		this.bmset[name] = struct{}{}
	}

	if idx == -1 {
		for idx := range this.b {
			if err := this.hit(name, idx, isFail); err != nil {
				return err
			}
		}
		return nil
	}
	return this.hit(name, idx, isFail)
}

func (this *BreakerManager) Status(name string) (int32, int) {
	this.rwLock.RLock()
	defer this.rwLock.RUnlock()
	if _, ok := this.bmset[name]; !ok {
		return cb.CircuitBreakerStatusClosed, 0
	}
	for idx, cbManager := range this.b {
		if cbManager[name].Status() != cb.CircuitBreakerStatusClosed {
			return cbManager[name].Status(), idx
		}
	}
	return cb.CircuitBreakerStatusClosed, 0
}

func (this *BreakerManager) CASHalfOpenProbeSent(name string, idx int, halfOpenProbeSent bool) bool {
	this.rwLock.Lock()
	defer this.rwLock.Unlock()
	if _, ok := this.bmset[name]; !ok {
		return false
	}
	if this.b[idx][name].GetHalfOpenProbeSent() == halfOpenProbeSent {
		return false
	}
	this.b[idx][name].SetHalfOpenProbeSent(halfOpenProbeSent)
	return true
}

func (this *BreakerManager) AddTimeWheelTask(name string, connectionID uint64, flag *int32) error {
	for idx, strategy := range this.strategies {
		hitNum := idx
		if err := this.tw.Add(strategy.sqlTimeoutMsDuration, uint64(connectionID)*this.hashFactor+uint64(hitNum), func() {
			atomic.AddInt32(flag, 1)
			this.Hit(name, hitNum, true)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (this *BreakerManager) RemoveTimeWheelTask(connectionID uint64) error {
	for idx := range this.strategies {
		if err := this.tw.Remove(uint64(connectionID)*this.hashFactor + uint64(idx)); err != nil {
			return err
		}
	}
	return nil
}

func (this *BreakerManager) CloseBreaker() {
	this.tw.Stop()
	this.b = nil
}
