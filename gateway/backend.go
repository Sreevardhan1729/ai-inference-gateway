package gateway

import (
	"math"
	"sync"
)

type Backend struct {
	URL           string
	weight        int
	currentWeight int
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

func (lb *LoadBalancer) Next() *Backend {
	lb.mx.Lock()
	defer lb.mx.Unlock()
	maxi := math.MinInt
	pickIdx := 0

	for i, backend := range lb.backends {
		backend.currentWeight += backend.weight
		if maxi < backend.currentWeight {
			maxi = backend.currentWeight
			pickIdx = i
		}

	}
	lb.backends[pickIdx].currentWeight -= lb.totalWeight

	return lb.backends[pickIdx]
}

func NewLoadBalancer(configs []BackendConfig) *LoadBalancer {
	backends := make([]*Backend, len(configs))
	totalWeight := 0
	for i, config := range configs {
		backends[i] = &Backend{URL: config.URL, weight: config.Weight}
		totalWeight += config.Weight
	}
	return &LoadBalancer{backends: backends, totalWeight: totalWeight}
}
