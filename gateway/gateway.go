package gateway

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type Gateway struct {
	LB     *LoadBalancer
	Client *http.Client
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var fullUrl string
	currentBackend := g.LB.Next()
	if currentBackend == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(r.URL.RawQuery) == 0 {
		fullUrl = fmt.Sprintf("%s%s", currentBackend.URL, r.URL.Path)
	} else {
		fullUrl = fmt.Sprintf("%s%s?%s", currentBackend.URL, r.URL.Path, r.URL.RawQuery)
	}
	req, errReq := http.NewRequestWithContext(r.Context(), r.Method, fullUrl, r.Body)
	if errReq != nil {
		log.Printf("Failed to create backend Request: %v", errReq)
		http.Error(w, "Internal Server Error: ", http.StatusInternalServerError)
		return
	}
	req.Header = r.Header.Clone()
	resp, errResp := g.Client.Do(req)
	if errResp != nil {
		log.Printf("Backend request Failed: %v", errResp)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		g.LB.IncrementFailureAndUpdateHealthStatus(currentBackend)
		return
	}
	if resp.StatusCode >= 500 {
		g.LB.IncrementFailureAndUpdateHealthStatus(currentBackend)
	} else {
		g.LB.RecordSuccess(currentBackend)
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		for _, value := range v {
			w.Header().Add(k, value)
		}
	}
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		flusher, ok := w.(http.Flusher)

		if !ok {
			http.Error(
				w,
				"Streaming unsupported",
				http.StatusInternalServerError,
			)
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		w.WriteHeader(resp.StatusCode)
		scanner.Buffer(
			make([]byte, 1024),
			1024*1024*10,
		)
		for scanner.Scan() {
			fmt.Fprintln(w, scanner.Text())
			flusher.Flush()
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Internal Server Error: Failed to encode response payload %v", err)
			return
		}
	} else {
		w.WriteHeader(resp.StatusCode)
		_, err := io.Copy(w, resp.Body)
		if err != nil {
			log.Printf("Internal Server Error: Failed to encode response payload %v", err)
			return
		}
	}
}
