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
	gw := &gateway.Gateway{
		LB:     lb,
		Client: &http.Client{},
	}
	go gateway.StartHealthChecker(lb, 10*time.Second)
	server := &http.Server{
		Addr:    ":8080",
		Handler: gw,
	}
	log.Println("Proxy running port: 8080")
	if errProxy := server.ListenAndServe(); errProxy != nil {
		log.Fatalf("Server error %v", errProxy)
	}
}
