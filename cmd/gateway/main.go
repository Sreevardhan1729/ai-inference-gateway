package main

import (
	"ai-inference-gateway/gateway"
	"log"
	"net/http"
)

func main() {
	gw := &gateway.Gateway{
		LB: gateway.NewLoadBalancer([]gateway.BackendConfig{
			{URL: "http://localhost:9001", Weight: 1},
			{URL: "http://localhost:9002", Weight: 3},
		}),
		Client: &http.Client{},
	}
	server := &http.Server{
		Addr:    ":8080",
		Handler: gw,
	}
	log.Println("Proxy running port: 8080")
	if errProxy := server.ListenAndServe(); errProxy != nil {
		log.Fatalf("Server error %v", errProxy)
	}
}
