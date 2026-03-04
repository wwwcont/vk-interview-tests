package infra

/*
Упрощённый Job Runner для интервью:
- in-memory repo + idempotency index;
- одна bounded очередь (без разделения high/normal);
- resize воркеров на лету;
- retry (max 3) с backoff 50/100/200ms;
- graceful shutdown: reject новых, queued -> canceled, running ждём до timeout.
*/

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"
	"vk-interview-tests/internal/domain"
)

type Repo struct {
	mu   sync.RWMutex
	jobs map[string]*domain.Job
	key  map[string]string
}

func NewRepo() *Repo               { return &Repo{jobs: map[string]*domain.Job{}, key: map[string]string{}} }
func cp(j *domain.Job) *domain.Job { c := *j; return &c }
func (r *Repo) Save(j *domain.Job) { r.mu.Lock(); r.jobs[j.ID] = cp(j); r.mu.Unlock() }
func (r *Repo) ByID(id string) (*domain.Job, error) {
	r.mu.RLock()
	j := r.jobs[id]
	r.mu.RUnlock()
	if j == nil {
		return nil, domain.ErrNotFound
	}
	return cp(j), nil
}
func (r *Repo) ByKey(k string) (*domain.Job, error) {
	r.mu.RLock()
	id := r.key[k]
	r.mu.RUnlock()
	if id == "" {
		return nil, domain.ErrNotFound
	}
	return r.ByID(id)
}
func (r *Repo) Bind(k, id string) {
	r.mu.Lock()
	if r.key[k] == "" {
		r.key[k] = id
	}
	r.mu.Unlock()
}

type worker struct{ stop chan struct{} }
type Pool struct {
	repo    *Repo
	mu      sync.Mutex
	cond    *sync.Cond
	cap     int
	down    bool
	q       []*domain.Job
	workers map[int]worker
	nextID  int
	wg      sync.WaitGroup
	running int
}

func NewPool(repo *Repo, n, cap int) *Pool {
	p := &Pool{repo: repo, cap: cap, workers: map[int]worker{}}
	p.cond = sync.NewCond(&p.mu)
	_ = p.Resize(n)
	return p
}
func (p *Pool) IsDown() bool { p.mu.Lock(); d := p.down; p.mu.Unlock(); return d }
func (p *Pool) Enqueue(ctx context.Context, j *domain.Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.down {
		return domain.ErrShuttingDown
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(p.q) >= p.cap {
		return domain.ErrQueueFull
	}
	p.q = append(p.q, j)
	p.cond.Signal()
	log.Printf("job created id=%s", j.ID)
	return nil
}
func (p *Pool) Resize(n int) error {
	if n <= 0 {
		return errors.New("workers must be >0")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := len(p.workers)
	for ; cur < n; cur++ {
		id := p.nextID
		p.nextID++
		w := worker{stop: make(chan struct{})}
		p.workers[id] = w
		p.wg.Add(1)
		go p.loop(w.stop)
	}
	for ; cur > n; cur-- {
		for id, w := range p.workers {
			close(w.stop)
			delete(p.workers, id)
			break
		}
	}
	p.cond.Broadcast()
	return nil
}
func (p *Pool) loop(stop <-chan struct{}) {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for len(p.q) == 0 && !p.down {
			p.cond.Wait()
		}
		if p.down && len(p.q) == 0 {
			p.mu.Unlock()
			return
		}
		select {
		case <-stop:
			p.mu.Unlock()
			return
		default:
		}
		j := p.q[0]
		p.q = p.q[1:]
		p.running++
		p.mu.Unlock()
		exec(j)
		p.repo.Save(j)
		p.mu.Lock()
		p.running--
		p.mu.Unlock()
	}
}
func exec(j *domain.Job) {
	n := time.Now().UTC()
	j.Status = domain.Running
	if j.StartedAt == nil {
		j.StartedAt = &n
	}
	log.Printf("job start id=%s", j.ID)
	var e error
	for a := 1; a <= 3; a++ {
		j.Attempts++
		e = run(j)
		if e == nil {
			j.Status = domain.Succeeded
			break
		}
		j.LastError = e.Error()
		if a < 3 {
			time.Sleep(time.Duration(50<<uint(a-1)) * time.Millisecond)
		}
	}
	if e != nil {
		j.Status = domain.Failed
	}
	f := time.Now().UTC()
	j.FinishedAt = &f
	log.Printf("job done id=%s status=%s", j.ID, j.Status)
}
func run(j *domain.Job) error {
	if j.Payload.Fail || j.Payload.FailAttempts >= j.Attempts {
		return errors.New("job failed")
	}
	time.Sleep(time.Duration(j.Payload.MS) * time.Millisecond)
	return nil
}
func (p *Pool) Shutdown(ctx context.Context) ([]string, error) {
	p.mu.Lock()
	if p.down {
		p.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	p.down = true
	for _, j := range p.q {
		j.Status = domain.Canceled
		f := time.Now().UTC()
		j.FinishedAt = &f
		j.LastError = "canceled on shutdown"
		p.repo.Save(j)
	}
	p.q = nil
	p.cond.Broadcast()
	p.mu.Unlock()
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil, nil
	case <-ctx.Done():
		p.mu.Lock()
		r := p.running
		p.mu.Unlock()
		return []string{"running:" + strconv.Itoa(r)}, ctx.Err()
	}
}
