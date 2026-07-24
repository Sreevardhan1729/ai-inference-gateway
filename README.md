# AI Inference Gateway

A high-performance reverse proxy for LLM APIs, built in Go. Routes requests across multiple backends using smooth weighted round-robin load balancing with streaming (SSE) support.

## Architecture

```
Client -> Gateway (:8080) -> LoadBalancer -> Backend A (:9001, weight=1)
                                          -> Backend B (:9002, weight=3)
```

- **Gateway** — HTTP reverse proxy that forwards requests and streams responses (SSE or standard JSON)
- **LoadBalancer** — smooth weighted round-robin (nginx-style), thread-safe via mutex
- **Mock Backend** — simulates an LLM API with configurable port, supports streaming and non-streaming responses

## Project Structure

```
cmd/
  gateway/main.go       # proxy entry point
  client/main.go        # test client
  mockbackend/main.go   # mock LLM backend
chat/
  chat.go               # request/response types (OpenAI-compatible)
gateway/
  gateway.go            # reverse proxy handler
  backend.go            # Backend, LoadBalancer, smooth weighted round-robin
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

Send a request:

```bash
go run ./cmd/client
```

Or with curl:

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"GPT","messages":[{"role":"user","content":"hello"}],"stream":false}'
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
