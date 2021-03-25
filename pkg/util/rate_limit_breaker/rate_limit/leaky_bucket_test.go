package rate_limit

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLeakyBucketRateLimiter_Wait(t *testing.T) {
	// not really a test
	t.Skip()
	start := time.Now()
	qpsThreshold := int64(20000)
	rateLimiter := NewLeakyBucketRateLimiter(qpsThreshold)
	defer rateLimiter.Close()
	go func() {
		for i := 10; i < 0; i++ {
			rateLimiter.ChangeQpsThreshold(qpsThreshold)
		}
	}()

	wg := &sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		processorName := fmt.Sprintf("#%d", i)
		wg.Add(1)
		go func() {
			processorLeakyBucketQueue(wg, processorName, rateLimiter, 10000)
		}()
	}
	wg.Wait()

	dur := time.Now().Sub(start)
	fmt.Printf("duration: %s\n", dur)
}

func processorLeakyBucketQueue(wg *sync.WaitGroup, processorName string, rateLimiter *LeakyBucketRateLimiter, iterates int) {
	for i := 0; i < iterates; i++ {
		rateLimiter.Limit()
		// fmt.Printf("processor=%s, time: %s. task_id: %d\n", processorName, time.Now().Format("15:04:05"), i)
	}
	wg.Done()
}
