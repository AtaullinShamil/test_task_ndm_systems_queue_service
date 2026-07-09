package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch      chan struct{}
	ctx     context.Context
	message string
	active  bool
}

type queue struct {
	messages []string
	waiters  []*waiter
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{
		mu:     sync.Mutex{},
		queues: make(map[string]*queue),
	}
}

func (b *broker) getQueue(name string) *queue {
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (b *broker) put(name, message string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getQueue(name)
	for len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		if !w.active {
			continue
		}
		if w.ctx.Err() != nil {
			w.active = false
			continue
		}
		w.active = false
		w.message = message
		close(w.ch)
		return
	}
	q.messages = append(q.messages, message)
}

func (b *broker) get(ctx context.Context, name string, wait bool) (string, bool) {
	b.mu.Lock()

	q := b.getQueue(name)
	if len(q.messages) > 0 {
		message := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		return message, true
	}
	if !wait {
		b.mu.Unlock()
		return "", false
	}

	w := &waiter{ch: make(chan struct{}), ctx: ctx, active: true}
	q.waiters = append(q.waiters, w)
	b.mu.Unlock()

	select {
	case <-w.ch:
		return w.message, true
	case <-ctx.Done():
		b.mu.Lock()
		if w.active {
			w.active = false
			for i, queued := range q.waiters {
				if queued == w {
					q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
					break
				}
			}
		} else {
			// a message may arrive at the same time as the timeout
			message := w.message
			b.mu.Unlock()
			return message, true
		}
		b.mu.Unlock()
		return "", false
	}
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[1:]

	switch r.Method {
	case http.MethodPut:
		values, ok := r.URL.Query()["v"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.put(name, values[0])
	case http.MethodGet:
		ctx := r.Context()
		wait := false
		if value := r.URL.Query().Get("timeout"); value != "" {
			seconds, err := strconv.Atoi(value)
			if err != nil || seconds < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
			defer cancel()
			wait = seconds > 0
		}

		message, ok := b.get(ctx, name, wait)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(message))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: queue-service <port>")
		os.Exit(1)
	}

	if err := http.ListenAndServe(":"+os.Args[1], newBroker()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
