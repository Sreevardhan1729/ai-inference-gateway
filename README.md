# AI Inference Gateway

A high-performance reverse proxy for LLM APIs, built in Go. Routes requests across multiple backends using smooth weighted round-robin load balancing with streaming (SSE) support, token-aware rate limiting, priority queuing with backpressure, Prometheus metrics, and per-key usage/cost tracking.

No external dependencies (stdlib only).

## Architecture

```
Client -> Gateway (:8080) -> RateLimiter (per-key token bucket)
                          -> PriorityQueue (bounded heap, 1000 cap, 50 workers)
                          -> LoadBalancer (smooth WRR + circuit breaker)
                              -> Backend A (:9001, weight=1)
                              -> Backend B (:9002, weight=3)
```

- **Gateway** (`gateway/gateway.go`) — HTTP reverse proxy. Enforces `X-API-Key` auth and token-bucket rate limiting, enqueues by `X-Priority`, forwards requests (method, path, query, headers, body) with context propagation, and streams responses back (SSE flush-per-chunk or JSON copy). Also serves `GET /metrics` and `GET /usage`.
- **LoadBalancer** (`gateway/backend.go`) — smooth weighted round-robin (nginx-style), thread-safe via mutex. Skips `Open` backends. With weights 1:3 the pick order is `B,B,A,B` repeating.
- **Circuit breaker** (`gateway/backend.go`, `gateway/healthcheck.go`) — per-backend `Closed/Open/HalfOpen` state. Opens after 5 consecutive failures; a background checker promotes `Open -> HalfOpen` after a 30s cooldown so one probe request can test recovery.
- **RateLimiter** (`gateway/ratelimiter.go`) — per-`X-API-Key` token buckets (`capacity=1000`, `refill=10/s`). Cost of a request is estimated as `MaxTokens + len(messages)/4`. Over budget returns `429 + Retry-After: 60`.
- **PriorityQueue** (`gateway/queue.go`) — bounded `container/heap` (1000 jobs) + 50 workers. Tiers from `X-Priority`: `premium=0, default=1, batch=2`; FIFO `Seq` tie-break prevents starvation. Non-blocking `Enqueue` sheds load fast (`429 + Retry-After: 5`) when full, and sheds `batch` jobs early at 80% full — this is the backpressure that keeps slow SSE streams from OOMing the gateway.
- **Metrics** (`gateway/metrics.go`) — stdlib-only Prometheus text exposition at `GET /metrics`: counters `llm_requests_total{backend,status,stream}`, `llm_tokens_total{key,direction}`, `llm_rejected_total{reason}`; histogram `llm_request_duration_seconds_*` (buckets from 5ms); gauges `llm_queue_depth`, `llm_inflight_requests`. API keys are hashed to 4 chars in labels to avoid high-cardinality series.
- **UsageTracker** (`gateway/usage.go`) — per-key input/output tokens, request/reject counts, and cost at `GET /usage?key=<api-key>` (or `GET /usage` for all). Pricing: `$0.50`/`$1.50` per 1M input/output tokens. Input is the pre-charge estimate; output is reconciled by parsing SSE `data:` frames (`message.content` / `delta.content` / `usage.completion_tokens`) or the JSON `usage` field via `io.TeeReader`.
- **Mock Backend** (`cmd/mockbackend/main.go`) — simulates an LLM API with configurable port, `/health`, and streaming/non-streaming responses. `NOSLEEP=1` env skips the artificial 1s delays (bench mode).

## Project Structure

```
cmd/
  gateway/main.go       # proxy entry point (wiring: LB, queue, metrics, usage, tuned transport)
  client/main.go        # test client (streaming request with X-API-Key)
  mockbackend/main.go   # mock LLM backend
chat/
  chat.go               # request/response types (OpenAI-compatible)
gateway/
  gateway.go            # reverse proxy handler + output-token reconciliation
  backend.go            # Backend, LoadBalancer, smooth WRR, circuit breaker
  ratelimiter.go        # per-key token buckets
  healthcheck.go        # background Open -> HalfOpen promotion
  queue.go              # bounded priority heap + workers (backpressure)
  metrics.go            # Prometheus RED metrics + /metrics exposition
  usage.go              # per-key usage/cost tracking + /usage
```

## Requirements

- Go 1.26+

No external dependencies.

## Setup

```bash
git clone <repo-url>
cd ai-inference-gateway
go build ./...
```

## Running

Start mock backends (separate terminals):

```bash
go run ./cmd/mockbackend        # runs on :9001
go run ./cmd/mockbackend 9002   # runs on :9002
```

Start the gateway:

```bash
go run ./cmd/gateway
```

Send a request (note: `X-API-Key` is required):

```bash
go run ./cmd/client
```

Or with curl:

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo" \
  -H "X-Priority: premium" \
  -d '{"model":"GPT","messages":[{"role":"user","content":"hello"}],"max_tokens":10,"stream":false}'
```

Streaming request:

```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo" \
  -d '{"model":"GPT","messages":[{"role":"user","content":"hello"}],"max_tokens":10,"stream":true}'
```

Observability:

```bash
curl -s http://localhost:8080/metrics | grep llm_
curl -s "http://localhost:8080/usage?key=demo"
```

## Configuration

Backend URLs and weights are configured in `cmd/gateway/main.go`:

```go
gateway.NewLoadBalancer([]gateway.BackendConfig{
    {URL: "http://localhost:9001", Weight: 1},
    {URL: "http://localhost:9002", Weight: 3},
})
```

Backend B receives 3x the traffic of Backend A.

Queue, transport, and rate-limit tuning live in the same file and `gateway/queue.go` / `gateway/ratelimiter.go`:

- Queue capacity 1000, batch shed at 80%, 50 workers
- Gateway→backend transport: `MaxIdleConnsPerHost: 512` (default 2 exhausts ephemeral ports under load), `Timeout: 60s`
- Token bucket: capacity 1000, refill 10/s

## Benchmarks

Loopback, Apple M1 / 16GB, instant mocks (`NOSLEEP=1`, small non-stream JSON), keep-alive — i.e. **proxy overhead only**, not LLM inference:

| Test | RPS | p50 | p99 | Errors |
|---|---|---|---|---|
| Direct → mock (backend ceiling) | ~61,400 | 0.7ms | 3.8ms | 0 |
| Via gateway, 64 conc, 20K reqs | ~17,500 | 2.8ms | 14.6ms | 0 |

p99 is cross-checked between the load generator and the `llm_request_duration_seconds` histogram (`histogram_quantile(0.99, rate(...[5m]))`). With real 1s inference or 6s SSE, throughput follows Little's Law (`concurrency = RPS x latency`), so scale workers + backends accordingly. At 256+ concurrency on a laptop expect ephemeral-port pressure — that is what the tuned transport and backpressure shedding (`429 + Retry-After`) are for.

## Known gaps

- `HalfOpen` probe handling in `LoadBalancer.Next()` and retry-to-next-backend failover are not yet implemented (currently returns `502` on dial error).
- `StartHealthChecker` does time-based promotion only; the `ping()` helper and `/health` probe are not yet wired into promotion decisions.
- `cmd/client` is a minimal demo, not a load generator.
