package gateway

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Price per 1M tokens (example GPT-4o-mini-ish rates, configurable).
const (
	PricePerMillionInput  = 0.50
	PricePerMillionOutput = 1.50
)

// UsageEntry is per-API-key cost attribution.
type UsageEntry struct {
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	Requests     int64   `json:"requests"`
	Rejected     int64   `json:"rejected"`
	CostUSD      float64 `json:"cost_usd"`
}

// UsageTracker tracks per-key token consumption for cost estimation.
// Hot path uses a single RWMutex; for >10k keys shard or use sync.Map.
type UsageTracker struct {
	mu      sync.RWMutex
	entries map[string]*UsageEntry
}

func NewUsageTracker() *UsageTracker {
	return &UsageTracker{entries: make(map[string]*UsageEntry)}
}

func costUSD(in, out int) float64 {
	return float64(in)/1e6*PricePerMillionInput + float64(out)/1e6*PricePerMillionOutput
}

// Record adds one completed request's input+output tokens.
func (u *UsageTracker) Record(key string, inputTokens, outputTokens int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	e, ok := u.entries[key]
	if !ok {
		e = &UsageEntry{}
		u.entries[key] = e
	}
	e.InputTokens += int64(inputTokens)
	e.OutputTokens += int64(outputTokens)
	e.Requests++
	e.CostUSD += costUSD(inputTokens, outputTokens)
}

func (u *UsageTracker) RecordRejected(key string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	e, ok := u.entries[key]
	if !ok {
		e = &UsageEntry{}
		u.entries[key] = e
	}
	e.Rejected++
}

func (u *UsageTracker) Get(key string) (UsageEntry, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	e, ok := u.entries[key]
	if !ok {
		return UsageEntry{}, false
	}
	return *e, true
}

// ServeUsage handles GET /usage?key=<api-key> plus GET /usage (all, for demo).
func (u *UsageTracker) ServeUsage(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	u.mu.RLock()
	defer u.mu.RUnlock()
	if key != "" {
		e, ok := u.entries[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			enc.Encode(map[string]string{"error": "unknown key"})
			return
		}
		enc.Encode(e)
		return
	}
	enc.Encode(u.entries)
}
