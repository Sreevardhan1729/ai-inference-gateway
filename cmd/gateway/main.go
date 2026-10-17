package main

import (
	"ai-inference-gateway/gateway"
	"log"
	"net/http"
	"time"
)

func main() {
	lb := gateway.NewLoadBalancer([]gateway.BackendConfig{
		{URL: "http://localhost:9001", Weight: 1},
		{URL: "http://localhost:9002", Weight: 3},
	})
	metrics := gateway.NewMetrics()
	usage := gateway.NewUsageTracker()
	// Bounded queue: 1000 jobs max, batch shed at 80%.
	// 50 workers cap concurrent backend dials (backpressure).
	queue := gateway.NewPriorityQueue(1000, metrics)
	queue.StartWorkers(50)
	gw := &gateway.Gateway{
		LB: lb,
		Client: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        2048,
				MaxIdleConnsPerHost: 512,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
		RL:      gateway.NewRateLimiter(),
		Queue:   queue,
		Metrics: metrics,
		Usage:   usage,
	}
	go gateway.StartHealthChecker(lb, 10*time.Second)
	server := &http.Server{
		Addr:              ":8080",
		Handler:           gw,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Println("Proxy running port: 8080")
	log.Println("Observability: GET /metrics (Prometheus), GET /usage?key=<api-key> (cost)")
	if errProxy := server.ListenAndServe(); errProxy != nil {
		log.Fatalf("Server error %v", errProxy)
	}
}
