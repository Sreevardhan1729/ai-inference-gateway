package gateway

import (
	"math"
	"sync"
	"time"
)

type CircuitState int

const (
	Closed CircuitState = iota
	Open
	HalfOpen
)

type Backend struct {
	URL            string
	weight         int
	currentWeight  int
	failureCounter int
	state          CircuitState
	openedAt       time.Time
}

type LoadBalancer struct {
	backends    []*Backend
	mx          sync.Mutex
	totalWeight int
}

type BackendConfig struct {
	URL    string
	Weight int
}

func (lb *LoadBalancer) IncrementFailureAndUpdateHealthStatus(b *Backend) {
	lb.mx.Lock()
	b.failureCounter += 1
	if b.failureCounter >= 5 {
		b.state = Open
		b.openedAt = time.Now()
	}
	lb.mx.Unlock()
}

func (lb *LoadBalancer) RecordSuccess(b *Backend) {
	lb.mx.Lock()
	b.state = Closed
	b.failureCounter = 0
	lb.mx.Unlock()
}

func (lb *LoadBalancer) Next() *Backend {
	lb.mx.Lock()
	defer lb.mx.Unlock()
	maxi := math.MinInt
	pickIdx := 0

	for i, backend := range lb.backends {
		if backend.state == Open {
			continue
		}
		backend.currentWeight += backend.weight
		if maxi < backend.currentWeight {
			maxi = backend.currentWeight
			pickIdx = i
		}
	}
	if maxi == math.MinInt {
		return nil
	}
	if lb.backends[pickIdx].state == HalfOpen {
		lb.backends[pickIdx].state = Open
	}
	lb.backends[pickIdx].currentWeight -= lb.totalWeight
	return lb.backends[pickIdx]
}

func NewLoadBalancer(configs []BackendConfig) *LoadBalancer {
	backends := make([]*Backend, len(configs))
	totalWeight := 0
	for i, config := range configs {
		backends[i] = &Backend{URL: config.URL, weight: config.Weight, state: Closed}
		totalWeight += config.Weight
	}
	return &LoadBalancer{backends: backends, totalWeight: totalWeight}
}
