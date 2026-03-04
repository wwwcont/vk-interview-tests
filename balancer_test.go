package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestPick_LeastInflight(t *testing.T) {
	b := NewBalancer()
	b.UpsertBackend("b1", true, 0, false)
	b.UpsertBackend("b2", true, 0, false)

	b.backends["b1"].inflight.Add(2)
	picked, ok := b.Pick("")
	if !ok {
		t.Fatal("expected a backend")
	}
	defer picked.inflight.Add(-1)
	if picked.id != "b2" {
		t.Fatalf("expected b2, got %s", picked.id)
	}
}

func TestHealth_UnhealthyNotPicked(t *testing.T) {
	b := NewBalancer()
	b.UpsertBackend("b1", false, 0, false)
	b.UpsertBackend("b2", true, 0, false)

	picked, ok := b.Pick("")
	if !ok {
		t.Fatal("expected a healthy backend")
	}
	defer picked.inflight.Add(-1)
	if picked.id != "b2" {
		t.Fatalf("expected b2, got %s", picked.id)
	}

	b.SetHealth("b2", false)
	if _, ok := b.Pick(""); ok {
		t.Fatal("expected no healthy backend")
	}
}

func TestFailover_SecondBackendUsed(t *testing.T) {
	b := NewBalancer()
	b.UpsertBackend("b1", true, 0, true)
	b.UpsertBackend("b2", true, 0, false)

	backendID, ok := b.DoRequest(context.Background())
	if !ok {
		t.Fatal("expected request to succeed on failover")
	}
	if backendID != "b2" {
		t.Fatalf("expected b2, got %s", backendID)
	}
	if fails := b.backends["b1"].fails.Load(); fails != 1 {
		t.Fatalf("expected b1 fails=1, got %d", fails)
	}
}

func TestTimeout_TriggersFailover(t *testing.T) {
	b := NewBalancer()
	b.UpsertBackend("slow", true, 200, false)
	b.UpsertBackend("fast", true, 0, false)

	backendID, ok := b.DoRequest(context.Background())
	if !ok {
		t.Fatal("expected request to succeed on second backend")
	}
	if backendID != "fast" {
		t.Fatalf("expected fast, got %s", backendID)
	}
	if fails := b.backends["slow"].fails.Load(); fails != 1 {
		t.Fatalf("expected slow fails=1, got %d", fails)
	}
}

func TestConcurrentRequests_InflightReturnsToZero(t *testing.T) {
	b := NewBalancer()
	b.UpsertBackend("b1", true, 10, false)
	b.UpsertBackend("b2", true, 10, false)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.DoRequest(context.Background())
		}()
	}
	wg.Wait()

	for _, s := range b.Snapshot() {
		if s.Inflight != 0 {
			t.Fatalf("backend %s inflight=%d, want 0", s.ID, s.Inflight)
		}
	}
}

func TestHTTP_RequestEndpoint(t *testing.T) {
	b := NewBalancer()
	mux := newMux(b)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	postJSON := func(path string, payload any) *http.Response {
		t.Helper()
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		return resp
	}

	resp := postJSON("/backends", backendUpsertRequest{ID: "slow", Healthy: true, DelayMS: 200})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upsert slow status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = postJSON("/backends", backendUpsertRequest{ID: "fast", Healthy: true, DelayMS: 0})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upsert fast status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	client := http.Client{Timeout: time.Second}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/request", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out requestResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !out.OK || out.BackendID != "fast" {
		t.Fatalf("want ok on fast backend, got ok=%v backend=%s", out.OK, out.BackendID)
	}
}
