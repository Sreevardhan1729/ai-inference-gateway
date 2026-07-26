package gateway

import (
	"sync"
	"time"
)

type TokenBucket struct {
	capacity      float64
	currentTokens float64
	refillRate    float64
	lastFill      time.Time
}

type RateLimiter struct {
	buckets map[string]*TokenBucket
	mu      sync.Mutex
}

func (rl *RateLimiter) Accept(key string, estimated int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	currBucket, exists := rl.buckets[key]
	if !exists {
		currBucket = &TokenBucket{
			capacity:      1000,
			currentTokens: 1000,
			refillRate:    10,
			lastFill:      time.Now(),
		}
		rl.buckets[key] = currBucket
	}
	elapsed := time.Since(currBucket.lastFill).Seconds()
	currBucket.currentTokens = min(currBucket.capacity, currBucket.currentTokens+(currBucket.refillRate*elapsed))
	currBucket.lastFill = time.Now()
	if float64(estimated) <= currBucket.currentTokens {
		currBucket.currentTokens -= float64(estimated)
		return true
	} else {
		return false
	}
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{buckets: make(map[string]*TokenBucket)}
}
