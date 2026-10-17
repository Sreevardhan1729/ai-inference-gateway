package gateway

import (
	"ai-inference-gateway/chat"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type Gateway struct {
	LB      *LoadBalancer
	RL      *RateLimiter
	Client  *http.Client
	Queue   *PriorityQueue
	Metrics *Metrics
	Usage   *UsageTracker
}

// estimateTokens is the ~4 chars/token heuristic for plain text.
func estimateTokens(s string) int { return len(s) / 4 }

func getEstimated(body []byte) int {
	var requstBody chat.ChatCompletionRequest
	err := json.NewDecoder(bytes.NewReader(body)).Decode(&requstBody)
	if err != nil {
		return -1
	}
	var contentLength int
	for _, request := range requstBody.Messages {
		contentLength += len(request.Content)
	}
	totalTokens := requstBody.MaxTokens + contentLength/4
	return totalTokens
}

// sseOutputTokens extracts completion text from one SSE "data: {...}" line.
// Handles both mock format (choices[].message.content) and OpenAI
// streaming format (choices[].delta.content). Falls back to raw length/4.
func sseOutputTokens(line string) int {
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if payload == "" || payload == "[DONE]" {
		return 0
	}
	var chunk struct {
		Choices []struct {
			Message *struct {
				Content string `json:"content"`
			} `json:"message"`
			Delta *struct {
				Content string `json:"content"`
			} `json:"delta"`
			Text string `json:"text"`
		} `json:"choices"`
		Usage *struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
		return estimateTokens(payload)
	}
	if chunk.Usage != nil && chunk.Usage.CompletionTokens > 0 {
		return chunk.Usage.CompletionTokens
	}
	total := 0
	for _, c := range chunk.Choices {
		if c.Message != nil {
			total += estimateTokens(c.Message.Content)
		}
		if c.Delta != nil {
			total += estimateTokens(c.Delta.Content)
		}
		if c.Text != "" {
			total += estimateTokens(c.Text)
		}
	}
	return total
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Observability endpoints (not proxied).
	if r.URL.Path == "/metrics" && g.Metrics != nil {
		g.Metrics.ServeHTTP(w, r)
		return
	}
	if r.URL.Path == "/usage" && g.Usage != nil {
		g.Usage.ServeUsage(w, r)
		return
	}

	start := time.Now()
	bodyBytes, _ := io.ReadAll(r.Body)
	defer r.Body.Close()
	user := r.Header.Get("X-API-Key")
	if user == "" {
		if g.Metrics != nil {
			g.Metrics.IncRejected("unauthorized")
		}
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	estimated := getEstimated(bodyBytes)
	if estimated == -1 {
		http.Error(w, "Internal Server Error: Failed to read request", http.StatusInternalServerError)
		return
	}
	if !g.RL.Accept(user, estimated) {
		if g.Metrics != nil {
			g.Metrics.IncRejected("rate_limited")
		}
		if g.Usage != nil {
			g.Usage.RecordRejected(user)
		}
		w.Header().Set("Retry-After", "60")
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}

	// Priority queuing with backpressure: run proxy work on a worker.
	// Backend selection happens inside the job (fresh view of circuit state).
	priority := ParsePriority(r.Header.Get("X-Priority"))
	proxyFn := func() { g.doProxy(w, r, bodyBytes, user, estimated, start) }

	if g.Queue != nil {
		done, ok := g.Queue.Enqueue(priority, proxyFn)
		if !ok {
			if g.Metrics != nil {
				g.Metrics.IncRejected("queue_full")
			}
			if g.Usage != nil {
				g.Usage.RecordRejected(user)
			}
			w.Header().Set("Retry-After", "5")
			if g.Metrics != nil {
				w.Header().Set("X-Queue-Depth", fmt.Sprint(g.Metrics.QueueDepth()))
			}
			http.Error(w, "Too Many Requests: server overloaded", http.StatusTooManyRequests)
			return
		}
		// Block handler goroutine until worker finishes. Writer is only
		// touched by the worker, so no concurrent writes.
		select {
		case <-done:
		case <-r.Context().Done():
			// Client went away while queued/executing; worker still
			// finishes (write will fail silently). Just stop waiting.
		}
		return
	}
	proxyFn()
}

// doProxy runs on a queue worker: pick backend, forward, stream back,
// reconcile output tokens, record metrics + per-key usage.
func (g *Gateway) doProxy(w http.ResponseWriter, r *http.Request, bodyBytes []byte, user string, estimated int, start time.Time) {
	if g.Metrics != nil {
		g.Metrics.IncInflight()
		defer g.Metrics.DecInflight()
	}
	currentBackend := g.LB.Next()
	if currentBackend == nil {
		g.finishMetrics(user, "none", "503", "false", estimated, 0, start)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	var fullUrl string
	if len(r.URL.RawQuery) == 0 {
		fullUrl = fmt.Sprintf("%s%s", currentBackend.URL, r.URL.Path)
	} else {
		fullUrl = fmt.Sprintf("%s%s?%s", currentBackend.URL, r.URL.Path, r.URL.RawQuery)
	}
	req, errReq := http.NewRequestWithContext(r.Context(), r.Method, fullUrl, bytes.NewReader(bodyBytes))
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
		g.finishMetrics(user, currentBackend.URL, "502", "false", estimated, 0, start)
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
	stream := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	streamLabel := "false"
	if stream {
		streamLabel = "true"
	}
	outputTokens := 0
	statusLabel := fmt.Sprint(resp.StatusCode)

	if stream {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}
		scanner := bufio.NewScanner(resp.Body)
		w.WriteHeader(resp.StatusCode)
		scanner.Buffer(make([]byte, 1024), 1024*1024*10)
		for scanner.Scan() {
			line := scanner.Text()
			outputTokens += sseOutputTokens(line)
			fmt.Fprintln(w, line)
			flusher.Flush()
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Internal Server Error: Failed to encode response payload %v", err)
		}
	} else {
		// Tee the body so we can meter output tokens without extra buffering.
		var buf bytes.Buffer
		w.WriteHeader(resp.StatusCode)
		_, err := io.Copy(w, io.TeeReader(resp.Body, &buf))
		if err != nil {
			log.Printf("Internal Server Error: Failed to encode response payload %v", err)
		}
		var chatResp chat.ChatCompletionResponse
		if json.Unmarshal(buf.Bytes(), &chatResp) == nil && chatResp.Usage.CompletionTokens > 0 {
			outputTokens = chatResp.Usage.CompletionTokens
		} else {
			outputTokens = estimateTokens(buf.String())
		}
	}
	g.finishMetrics(user, currentBackend.URL, statusLabel, streamLabel, estimated, outputTokens, start)
}

func (g *Gateway) finishMetrics(user, backend, status, stream string, in, out int, start time.Time) {
	if g.Metrics != nil {
		g.Metrics.IncRequests(backend, status, stream)
		g.Metrics.AddTokens(hashKey(user), "in", in)
		g.Metrics.AddTokens(hashKey(user), "out", out)
		g.Metrics.ObserveLatency(time.Since(start).Seconds())
	}
	if g.Usage != nil {
		g.Usage.Record(user, in, out)
	}
}
