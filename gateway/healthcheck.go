package gateway

import (
	"net/http"
	"time"
)

func ping(client *http.Client, url string) bool {
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
func StartHealthChecker(lb *LoadBalancer, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		for _, backend := range lb.backends {
			lb.mx.Lock()
			if backend.state == Open && time.Since(backend.openedAt) > 30*time.Second {
				backend.state = HalfOpen
			}
			lb.mx.Unlock()

		}
	}
}
