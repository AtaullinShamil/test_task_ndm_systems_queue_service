package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestQueueFullCycle(t *testing.T) {
	server := httptest.NewServer(newBroker())
	defer server.Close()

	request := func(method, path string) (int, string) {
		t.Helper()

		req, err := http.NewRequest(method, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		return resp.StatusCode, string(body)
	}

	if status, body := request(http.MethodPut, "/pet?v=cat"); status != http.StatusOK || body != "" {
		t.Fatalf("PUT cat: got status %d, body %q", status, body)
	}
	if status, body := request(http.MethodPut, "/pet?v=dog"); status != http.StatusOK || body != "" {
		t.Fatalf("PUT dog: got status %d, body %q", status, body)
	}
	if status, body := request(http.MethodPut, "/pet"); status != http.StatusBadRequest || body != "" {
		t.Fatalf("PUT without v: got status %d, body %q", status, body)
	}
	if status, body := request(http.MethodGet, "/pet"); status != http.StatusOK || body != "cat" {
		t.Fatalf("GET first pet: got status %d, body %q", status, body)
	}
	if status, body := request(http.MethodGet, "/pet"); status != http.StatusOK || body != "dog" {
		t.Fatalf("GET second pet: got status %d, body %q", status, body)
	}
	if status, body := request(http.MethodGet, "/pet"); status != http.StatusNotFound || body != "" {
		t.Fatalf("GET empty pet: got status %d, body %q", status, body)
	}
	if status, body := request(http.MethodGet, "/pet?timeout=1"); status != http.StatusNotFound || body != "" {
		t.Fatalf("GET timeout: got status %d, body %q", status, body)
	}

	first := make(chan string, 1)
	second := make(chan string, 1)

	go func() {
		_, body := request(http.MethodGet, "/role?timeout=2")
		first <- body
	}()
	time.Sleep(100 * time.Millisecond)
	go func() {
		_, body := request(http.MethodGet, "/role?timeout=2")
		second <- body
	}()
	time.Sleep(100 * time.Millisecond)

	request(http.MethodPut, "/role?v=manager")
	request(http.MethodPut, "/role?v=executive")

	if body := <-first; body != "manager" {
		t.Fatalf("first waiter got %q", body)
	}
	if body := <-second; body != "executive" {
		t.Fatalf("second waiter got %q", body)
	}
}

func TestPutSkipsExpiredWaiter(t *testing.T) {
	b := newBroker()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b.mu.Lock()
	q := b.getQueue("pet")
	q.waiters = append(q.waiters, &waiter{
		ch:     make(chan struct{}),
		ctx:    ctx,
		active: true,
	})
	b.mu.Unlock()

	b.put("pet", "cat")

	message, ok := b.get(context.Background(), "pet", false)
	if !ok || message != "cat" {
		t.Fatalf("message was lost: got %q, ok %v", message, ok)
	}
}
