package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"vk-interview-tests/internal/domain"
	"vk-interview-tests/internal/httpapi"
	"vk-interview-tests/internal/service"
)

func newService() *service.Service { return service.New(domain.NewBalancer()) }

func TestPick_LeastInflight(t *testing.T) {
	b := domain.NewBalancer()
	b.Upsert("b1", true, 0, false)
	b.Upsert("b2", true, 0, false)
	b1, ok := b.Pick("b2") // force pick b1
	if !ok {
		t.Fatal("expected b1")
	}
	b1.Inflight.Add(2) // make b1 busier
	defer b1.Inflight.Add(-3)

	picked, ok := b.Pick("")
	if !ok {
		t.Fatal("expected backend")
	}
	defer picked.Inflight.Add(-1)
	if picked.ID != "b2" {
		t.Fatalf("want b2 got %s", picked.ID)
	}
}

func TestHealth_UnhealthyNotPicked(t *testing.T) {
	b := domain.NewBalancer()
	b.Upsert("b1", false, 0, false)
	b.Upsert("b2", true, 0, false)
	picked, ok := b.Pick("")
	if !ok || picked.ID != "b2" {
		t.Fatalf("want b2, ok=%v, got=%v", ok, picked)
	}
	picked.Inflight.Add(-1)
	b.SetHealth("b2", false)
	if _, ok := b.Pick(""); ok {
		t.Fatal("unexpected healthy backend")
	}
}

func TestFailover_SecondBackendUsed(t *testing.T) {
	s := newService()
	s.UpsertBackend("b1", true, 0, true)
	s.UpsertBackend("b2", true, 0, false)
	id, err := s.DoRequest(context.Background())
	if err != nil || id != "b2" {
		t.Fatalf("want b2 with nil err, got id=%s err=%v", id, err)
	}
}

func TestTimeout_TriggersFailover(t *testing.T) {
	s := newService()
	s.UpsertBackend("slow", true, 200, false)
	s.UpsertBackend("fast", true, 0, false)
	id, err := s.DoRequest(context.Background())
	if err != nil || id != "fast" {
		t.Fatalf("want fast with nil err, got id=%s err=%v", id, err)
	}
}

func TestConcurrentRequests_InflightReturnsToZero(t *testing.T) {
	s := newService()
	s.UpsertBackend("b1", true, 10, false)
	s.UpsertBackend("b2", true, 10, false)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = s.DoRequest(context.Background()) }()
	}
	wg.Wait()
	for _, st := range s.Stats() {
		if st.Inflight != 0 {
			t.Fatalf("backend %s inflight=%d", st.ID, st.Inflight)
		}
	}
}

func TestHTTP_RequestEndpoint(t *testing.T) {
	s := newService()
	ts := httptest.NewServer(httpapi.NewMux(s))
	defer ts.Close()

	post := func(path string, body any) int {
		b, _ := json.Marshal(body)
		resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("/backends", map[string]any{"id": "slow", "healthy": true, "delay_ms": 200}); code != http.StatusOK {
		t.Fatalf("upsert slow code=%d", code)
	}
	if code := post("/backends", map[string]any{"id": "fast", "healthy": true, "delay_ms": 0}); code != http.StatusOK {
		t.Fatalf("upsert fast code=%d", code)
	}

	client := http.Client{Timeout: time.Second}
	resp, err := client.Post(ts.URL+"/request", "application/json", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out struct {
		BackendID string `json:"backend_id"`
		OK        bool   `json:"ok"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.OK || out.BackendID != "fast" {
		t.Fatalf("want fast+ok, got id=%s ok=%v", out.BackendID, out.OK)
	}
}
