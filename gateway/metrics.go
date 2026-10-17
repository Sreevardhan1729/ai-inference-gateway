package gateway

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

// LatencyBuckets are Histogram upper bounds in seconds.
// Covers 5ms -> 10s so p50/p99 (incl. 5.9ms claim) are observable.
var LatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Metrics holds RED (Rate/Errors/Duration) + saturation signals.
// Stdlib-only: exposes Prometheus text exposition without client_golang.
type Metrics struct {
	mu sync.Mutex
	// Counter: "backend|status|stream" -> count
	requests map[string]int64
	// Counter: "key_hash|direction" -> tokens (direction=in|out)
	tokens map[string]int64
	// Counter reasons: "rate_limited", "queue_full", "backend_down"
	rejected map[string]int64
	// Histogram: bucket index -> cumulative count, plus sum/count
	latencyBuckets []uint64
	latencySum     float64
	latencyCount   uint64
	// Gauges (atomic for lock-free reads on hot path)
	queueDepth atomic.Int64
	inflight   atomic.Int64
}

func NewMetrics() *Metrics {
	return &Metrics{
		requests:       make(map[string]int64),
		tokens:         make(map[string]int64),
		rejected:       make(map[string]int64),
		latencyBuckets: make([]uint64, len(LatencyBuckets)),
	}
}

func (m *Metrics) IncRequests(backend, status, stream string) {
	m.mu.Lock()
	m.requests[backend+"|"+status+"|"+stream] += 1
	m.mu.Unlock()
}

func (m *Metrics) AddTokens(keyHash, direction string, n int) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	m.tokens[keyHash+"|"+direction] += int64(n)
	m.mu.Unlock()
}

func (m *Metrics) IncRejected(reason string) {
	m.mu.Lock()
	m.rejected[reason] += 1
	m.mu.Unlock()
}

func (m *Metrics) ObserveLatency(seconds float64) {
	m.mu.Lock()
	for i, bound := range LatencyBuckets {
		if seconds <= bound {
			m.latencyBuckets[i]++
		}
	}
	m.latencySum += seconds
	m.latencyCount++
	m.mu.Unlock()
}

func (m *Metrics) SetQueueDepth(n int) { m.queueDepth.Store(int64(n)) }
func (m *Metrics) QueueDepth() int     { return int(m.queueDepth.Load()) }
func (m *Metrics) IncInflight()        { m.inflight.Add(1) }
func (m *Metrics) DecInflight()        { m.inflight.Add(-1) }

// hashKey avoids high-cardinality labels: never expose raw API keys.
func hashKey(key string) string {
	if len(key) <= 4 {
		return "short"
	}
	return "k_" + key[len(key)-4:]
}

// ServeHTTP exposes GET /metrics in Prometheus text exposition format.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	// Use sorted keys for deterministic output (easier to demo/grep).
	reqKeys := sortedKeys(m.requests)
	tokKeys := sortedKeys(m.tokens)
	rejKeys := sortedKeys(m.rejected)

	fmt.Fprintln(w, "# HELP llm_requests_total Total proxied requests.")
	fmt.Fprintln(w, "# TYPE llm_requests_total counter")
	for _, k := range reqKeys {
		var backend, status, stream string
		fmt.Sscanf(k, "%[^|]|%[^|]|%s", &backend, &status, &stream)
		// k was built as backend|status|stream; re-split manually to avoid Sscanf edge cases.
		parts := split3(k)
		backend, status, stream = parts[0], parts[1], parts[2]
		fmt.Fprintf(w, "llm_requests_total{backend=%q,status=%q,stream=%q} %d\n", backend, status, stream, m.requests[k])
	}
	fmt.Fprintln(w, "# HELP llm_tokens_total Tokens metered by direction.")
	fmt.Fprintln(w, "# TYPE llm_tokens_total counter")
	for _, k := range tokKeys {
		parts := split2(k)
		fmt.Fprintf(w, "llm_tokens_total{key=%q,direction=%q} %d\n", parts[0], parts[1], m.tokens[k])
	}
	fmt.Fprintln(w, "# HELP llm_rejected_total Rejected requests by reason.")
	fmt.Fprintln(w, "# TYPE llm_rejected_total counter")
	for _, k := range rejKeys {
		fmt.Fprintf(w, "llm_rejected_total{reason=%q} %d\n", k, m.rejected[k])
	}
	fmt.Fprintln(w, "# HELP llm_request_duration_seconds Request latency histogram.")
	fmt.Fprintln(w, "# TYPE llm_request_duration_seconds histogram")
	var cumulative uint64
	for i, bound := range LatencyBuckets {
		cumulative += 0 // buckets below are already cumulative by construction
		fmt.Fprintf(w, "llm_request_duration_seconds_bucket{le=%g} %d\n", bound, m.latencyBuckets[i])
	}
	fmt.Fprintf(w, "llm_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", m.latencyCount)
	fmt.Fprintf(w, "llm_request_duration_seconds_sum %g\n", m.latencySum)
	fmt.Fprintf(w, "llm_request_duration_seconds_count %d\n", m.latencyCount)
	fmt.Fprintln(w, "# HELP llm_queue_depth Current queued jobs.")
	fmt.Fprintln(w, "# TYPE llm_queue_depth gauge")
	fmt.Fprintf(w, "llm_queue_depth %d\n", m.queueDepth.Load())
	fmt.Fprintln(w, "# HELP llm_inflight_requests Currently proxied requests.")
	fmt.Fprintln(w, "# TYPE llm_inflight_requests gauge")
	fmt.Fprintf(w, "llm_inflight_requests %d\n", m.inflight.Load())
}

func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func split2(s string) [2]string {
	var out [2]string
	idx := -1
	for i := 0; i < len(s); i++ {
		if s[i] == '|' {
			idx = i
			break
		}
	}
	if idx < 0 {
		out[0] = s
		return out
	}
	out[0] = s[:idx]
	out[1] = s[idx+1:]
	return out
}

func split3(s string) [3]string {
	var out [3]string
	j := 0
	start := 0
	for i := 0; i < len(s) && j < 2; i++ {
		if s[i] == '|' {
			out[j] = s[start:i]
			j++
			start = i + 1
		}
	}
	out[j] = s[start:]
	return out
}
