package gateway

import (
	"container/heap"
	"sync"
	"sync/atomic"
)

// Priority tiers: lower value = served first.
// Client sends X-Priority: premium|default|batch (default if absent).
const (
	PriorityPremium = 0
	PriorityDefault = 1
	PriorityBatch   = 2
)

// ParsePriority maps the X-Priority header to a tier.
func ParsePriority(h string) int {
	switch h {
	case "premium":
		return PriorityPremium
	case "batch":
		return PriorityBatch
	default:
		return PriorityDefault
	}
}

// Job is one queued proxy request. fn runs on a worker; done is closed when fn returns.
type Job struct {
	Priority int
	Seq      uint64 // FIFO tie-breaker within same priority (prevents starvation)
	Fn       func()
	Done     chan struct{}
	index    int // heap index (required by heap.Interface)
}

type jobHeap []*Job

func (h jobHeap) Len() int { return len(h) }
func (h jobHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority < h[j].Priority
	}
	return h[i].Seq < h[j].Seq
}
func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *jobHeap) Push(x any) {
	job := x.(*Job)
	job.index = len(*h)
	*h = append(*h, job)
}
func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return job
}

// PriorityQueue is a bounded priority heap with backpressure.
// Enqueue is non-blocking: it returns false (shed load) instead of
// growing unbounded and OOMing under slow SSE streams.
type PriorityQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	pq       jobHeap
	capacity int
	seq      atomic.Uint64
	closed   bool
	metrics  *Metrics
}

func NewPriorityQueue(capacity int, metrics *Metrics) *PriorityQueue {
	q := &PriorityQueue{capacity: capacity, metrics: metrics}
	q.cond = sync.NewCond(&q.mu)
	heap.Init(&q.pq)
	return q
}

func (q *PriorityQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pq)
}

// Enqueue adds fn at priority. Returns (done, true) or (nil, false) if shed.
// Backpressure policy: hard reject when full; soft reject for batch jobs
// once 80% full so premium/default survive overload (load shedding).
func (q *PriorityQueue) Enqueue(priority int, fn func()) (chan struct{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, false
	}
	if len(q.pq) >= q.capacity {
		return nil, false
	}
	if priority == PriorityBatch && len(q.pq) >= q.capacity*4/5 {
		return nil, false
	}
	done := make(chan struct{})
	seq := q.seq.Add(1)
	heap.Push(&q.pq, &Job{Priority: priority, Seq: seq, Fn: fn, Done: done})
	if q.metrics != nil {
		q.metrics.SetQueueDepth(len(q.pq))
	}
	q.cond.Signal()
	return done, true
}

func (q *PriorityQueue) pop() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.pq) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.pq) == 0 {
		return nil
	}
	job := heap.Pop(&q.pq).(*Job)
	if q.metrics != nil {
		q.metrics.SetQueueDepth(len(q.pq))
	}
	return job
}

// StartWorkers launches n goroutines that pop highest-priority jobs first.
func (q *PriorityQueue) StartWorkers(n int) {
	for i := 0; i < n; i++ {
		go func() {
			for {
				job := q.pop()
				if job == nil {
					return
				}
				job.Fn()
				close(job.Done)
			}
		}()
	}
}

func (q *PriorityQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
