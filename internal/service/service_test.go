package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vk-interview-tests/internal/domain"
	"vk-interview-tests/internal/httpapi"
	"vk-interview-tests/internal/service"
)

type mockInvoker struct {
	calls int64
	err   error
	delay time.Duration
}

func (m *mockInvoker) Invoke(ctx context.Context) error {
	atomic.AddInt64(&m.calls, 1)
	if m.delay == 0 {
		return m.err
	}
	select {
	case <-time.After(m.delay):
		return m.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newService() *service.Service { return service.New(domain.NewBalancer()) }

func TestPick_LeastInflight_FailsAndLatencyAsTieBreaker(t *testing.T) {
	b := domain.NewBalancer()
	b.Upsert("a", true, 0, false)
	b.Upsert("b", true, 0, false)

	a, _ := b.Pick("b")
	a.Inflight.Add(-1)
	a.Fails.Add(2)
	a.LatencyNS.Store(int64(20 * time.Millisecond))

	picked, ok := b.Pick("")
	if !ok {
		t.Fatal("expected backend")
	}
	defer picked.Inflight.Add(-1)
	if picked.ID != "b" {
		t.Fatalf("want b, got %s", picked.ID)
	}
}

func TestHealth_UnhealthyNotPicked(t *testing.T) {
	s := newService()
	s.UpsertBackend("b1", true, 0, false)
	s.UpsertBackend("b2", true, 0, false)
	if ok := s.SetBackendHealth("b1", false); !ok {
		t.Fatal("set health failed")
	}

	for i := 0; i < 3; i++ {
		id, err := s.DoRequest(context.Background())
		if err != nil || id != "b2" {
			t.Fatalf("want b2 only, got id=%s err=%v", id, err)
		}
	}
}

func TestFailover_SecondBackendUsed_WithMock(t *testing.T) {
	s := newService()
	fail := &mockInvoker{err: errors.New("boom")}
	ok := &mockInvoker{}
	s.UpsertBackendWithInvoker("b1", true, fail)
	s.UpsertBackendWithInvoker("b2", true, ok)

	id, err := s.DoRequest(context.Background())
	if err != nil || id != "b2" {
		t.Fatalf("want b2, got id=%s err=%v", id, err)
	}
	if atomic.LoadInt64(&fail.calls) != 1 || atomic.LoadInt64(&ok.calls) != 1 {
		t.Fatalf("unexpected call counts fail=%d ok=%d", fail.calls, ok.calls)
	}
}

func TestTimeout_TriggersFailover_WithMock(t *testing.T) {
	s := newService()
	slow := &mockInvoker{delay: 200 * time.Millisecond}
	fast := &mockInvoker{}
	s.UpsertBackendWithInvoker("slow", true, slow)
	s.UpsertBackendWithInvoker("fast", true, fast)

	id, err := s.DoRequest(context.Background())
	if err != nil || id != "fast" {
		t.Fatalf("want fast, got id=%s err=%v", id, err)
	}
	if atomic.LoadInt64(&slow.calls) != 1 || atomic.LoadInt64(&fast.calls) != 1 {
		t.Fatalf("unexpected call counts slow=%d fast=%d", slow.calls, fast.calls)
	}
}

func TestConcurrentRequests_InflightReturnsToZero(t *testing.T) {
	s := newService()
	s.UpsertBackend("b1", true, 10, false)
	s.UpsertBackend("b2", true, 10, false)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
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

func TestHTTP_SetHealthFalseAndRequest(t *testing.T) {
	s := newService()
	ts := httptest.NewServer(httpapi.NewMux(s))
	defer ts.Close()

	post := func(path string, body any) (*http.Response, map[string]any) {
		b, _ := json.Marshal(body)
		resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		return resp, out
	}

	if resp, _ := post("/backends", map[string]any{"id": "b1", "healthy": true}); resp.StatusCode != http.StatusOK {
		t.Fatalf("upsert b1 status=%d", resp.StatusCode)
	}
	if resp, _ := post("/backends", map[string]any{"id": "b2", "healthy": true}); resp.StatusCode != http.StatusOK {
		t.Fatalf("upsert b2 status=%d", resp.StatusCode)
	}
	if resp, _ := post("/backends/b1/health", map[string]any{"healthy": false}); resp.StatusCode != http.StatusOK {
		t.Fatalf("set health status=%d", resp.StatusCode)
	}

	resp, err := http.Post(ts.URL+"/request", "application/json", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request status=%d", resp.StatusCode)
	}
	var out struct {
		BackendID string `json:"backend_id"`
		OK        bool   `json:"ok"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.OK || out.BackendID != "b2" {
		t.Fatalf("want b2/ok, got id=%s ok=%v", out.BackendID, out.OK)
	}
}
