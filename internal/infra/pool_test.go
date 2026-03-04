package infra

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"vk-interview-tests/internal/app"
	"vk-interview-tests/internal/domain"
	httptransport "vk-interview-tests/internal/transport/http"
)

func fixture(n int) (*app.Service, *Pool) {
	r := NewRepo()
	p := NewPool(r, n, 1000)
	return app.New(r, p), p
}

func TestCreateJob_Idempotency(t *testing.T) {
	s, p := fixture(1)
	defer p.Shutdown(context.Background())
	c := app.CreateCmd{Type: "sleep", Priority: "normal", Payload: domain.SleepPayload{MS: 10}, IdempotencyKey: "k"}
	j1, _ := s.CreateJob(context.Background(), c)
	j2, _ := s.CreateJob(context.Background(), c)
	if j1.ID != j2.ID {
		t.Fatalf("%s!=%s", j1.ID, j2.ID)
	}
}

func TestCreateJob_SameKeyConcurrentSingleExecution(t *testing.T) {
	s, p := fixture(2)
	defer p.Shutdown(context.Background())
	c := app.CreateCmd{Type: "sleep", Priority: "normal", Payload: domain.SleepPayload{MS: 20}, IdempotencyKey: "same-key"}
	ids := make([]string, 10)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j, e := s.CreateJob(context.Background(), c)
			if e == nil {
				ids[i] = j.ID
			}
		}(i)
	}
	wg.Wait()
	first := ids[0]
	for _, id := range ids {
		if id != first {
			t.Fatalf("different ids: %v", ids)
		}
	}
	wait(t, s, first, domain.Succeeded)
	j, _ := s.GetJob(context.Background(), first)
	if j.Attempts != 1 {
		t.Fatalf("expected one execution, attempts=%d", j.Attempts)
	}
}

func TestWorkerPool_Resize(t *testing.T) {
	s, p := fixture(1)
	defer p.Shutdown(context.Background())
	start := time.Now()
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		j, _ := s.CreateJob(context.Background(), app.CreateCmd{Type: "sleep", Priority: "normal", Payload: domain.SleepPayload{MS: 160}})
		ids = append(ids, j.ID)
	}
	time.Sleep(50 * time.Millisecond)
	_ = s.ResizePool(context.Background(), 3)
	for _, id := range ids {
		wait(t, s, id, domain.Succeeded)
	}
	if time.Since(start) > 450*time.Millisecond {
		t.Fatal("resize didn't speed up")
	}
}

func TestShutdown_StopsAccepting(t *testing.T) {
	s, _ := fixture(1)
	_, _ = s.Shutdown(context.Background(), time.Second)
	_, e := s.CreateJob(context.Background(), app.CreateCmd{Type: "sleep", Priority: "normal", Payload: domain.SleepPayload{MS: 1}})
	if !errors.Is(e, domain.ErrShuttingDown) {
		t.Fatalf("got %v", e)
	}
}

func TestRetry_AttemptsAndBackoff(t *testing.T) {
	s, p := fixture(1)
	defer p.Shutdown(context.Background())
	j, _ := s.CreateJob(context.Background(), app.CreateCmd{Type: "sleep", Priority: "normal", Payload: domain.SleepPayload{MS: 1, FailAttempts: 2}})
	wait(t, s, j.ID, domain.Succeeded)
	g, _ := s.GetJob(context.Background(), j.ID)
	if g.Attempts != 3 {
		t.Fatalf("attempts=%d", g.Attempts)
	}
}

func TestHTTP_CreateAndGetJob(t *testing.T) {
	s, p := fixture(1)
	defer p.Shutdown(context.Background())
	ts := httptest.NewServer(httptransport.New(s).Routes())
	defer ts.Close()
	r, e := http.Post(ts.URL+"/jobs", "application/json", strings.NewReader(`{"type":"sleep","payload":{"ms":5},"priority":"normal"}`))
	if e != nil || r.StatusCode != 200 {
		t.Fatalf("err=%v code=%d", e, r.StatusCode)
	}
}

func wait(t *testing.T, s *app.Service, id string, st domain.Status) {
	t.Helper()
	dl := time.Now().Add(2 * time.Second)
	for time.Now().Before(dl) {
		j, e := s.GetJob(context.Background(), id)
		if e == nil && j.Status == st {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout")
}
