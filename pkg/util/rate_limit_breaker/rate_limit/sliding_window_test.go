package rate_limit

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSlidingWindowRateLimiter_Limit(t *testing.T) {
	rl := NewSlidingWindowRateLimiter(10)
	ch := make(chan int)
	go func() {
		for i := 0; i < 100; i++ {
			ch <- i
		}
	}()

	// 使用 timer 确保只处理 1s 之内的数据
OUTER:
	for {
		select {
		case <-time.NewTimer(time.Second).C: // time is up
			break OUTER
		case i := <-ch:
			err := rl.Limit()
			if i < 10 {
				assert.Nil(t, err)
			} else {
				assert.Equal(t, err, ErrRateLimited)
				if i == 99 {
					break OUTER
				}
			}
		}
	}
}

func TestSlidingWindowRateLimiter_ChangeQpsThreshold(t *testing.T) {
	// 测试 ChangeQpsThreshold。qpsThreshold 一开始为 10
	// 1)发送 100 个请求，前 10 个通过，其他被限流
	// 2)等待 1s 并将 qpsThreshold 置为 20
	// 3)发送 100 个请求，前 20 个通过，其他被限流
	rl := NewSlidingWindowRateLimiter(10)
	ch := make(chan int)
	go func() {
		for i := 0; i < 100; i++ {
			ch <- i
		}

		// 等待 1s，将 qps 阈值置为 20
		<-time.NewTimer(time.Second).C
		rl.ChangeQpsThreshold(20)
		for i := 100; i < 200; i++ {
			ch <- i
		}
	}()

OUTER:
	for {
		select {
		case <-time.NewTimer(time.Second * 2).C: // time is up
			break OUTER
		case i := <-ch:
			err := rl.Limit()
			if i < 100 { // 前 100 个，10 个通过
				if i < 10 {
					assert.Nil(t, err)
				} else {
					assert.Equal(t, err, ErrRateLimited)
				}
			} else { // 后 100 个，20 个通过
				if i < 120 {
					assert.Nil(t, err)
				} else {
					assert.Equal(t, err, ErrRateLimited)
					if i == 199 {
						break OUTER
					}
				}
			}
		}
	}
}

func TestSlidingWindowRateLimiter_ConcurrentLimit(t *testing.T) {
	rl := NewSlidingWindowRateLimiter(20000)
	resultCh := make(chan int)
	const GoRoutines = 100
	for i := 0; i < GoRoutines; i++ {
		go func() {
			passedCount := 0
			for j := 0; j < 1000; j++ {
				err := rl.Limit()
				if err == nil {
					passedCount++
				}
			}
			resultCh <- passedCount
		}()
	}

	sum := 0
	i := 0
	for {
		count := <-resultCh
		sum += count
		i++
		if i >= GoRoutines {
			break
		}
	}
	assert.Equal(t, sum, 20000)
}
