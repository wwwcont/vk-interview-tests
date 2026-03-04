package infra

import (
	"context"
	"errors"
	"expvar"
	"log"
	"sync"
	"time"
	"vk-interview-tests/internal/domain"
)

type Repo struct {
	mu   sync.RWMutex
	jobs map[string]*domain.Job
	key  map[string]string
}

func NewRepo() *Repo                  { return &Repo{jobs: map[string]*domain.Job{}, key: map[string]string{}} }
func clone(j *domain.Job) *domain.Job { c := *j; return &c }
func (r *Repo) Save(j *domain.Job)    { r.mu.Lock(); r.jobs[j.ID] = clone(j); r.mu.Unlock() }
func (r *Repo) ByID(id string) (*domain.Job, error) {
	r.mu.RLock()
	j := r.jobs[id]
	r.mu.RUnlock()
	if j == nil {
		return nil, domain.ErrNotFound
	}
	return clone(j), nil
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

type Pool struct {
	repo                             *Repo
	mu                               sync.Mutex
	down                             bool
	cap                              int
	hq, nq                           chan *domain.Job
	ctx                              context.Context
	cancel                           context.CancelFunc
	wg                               sync.WaitGroup
	workers                          map[int]chan struct{}
	wid                              int
	active                           map[string]struct{}
	created, succ, fail, run, qh, qn *expvar.Int
}

func NewPool(r *Repo, n, cap int) *Pool {
	c, x := context.WithCancel(context.Background())
	p := &Pool{repo: r, cap: cap, hq: make(chan *domain.Job, cap), nq: make(chan *domain.Job, cap), ctx: c, cancel: x, workers: map[int]chan struct{}{}, active: map[string]struct{}{}, created: m("jobs_created_total"), succ: m("jobs_succeeded_total"), fail: m("jobs_failed_total"), run: m("jobs_running"), qh: m("queue_high_len"), qn: m("queue_normal_len")}
	_ = p.Resize(n)
	return p
}
func m(n string) *expvar.Int {
	if v := expvar.Get(n); v != nil {
		return v.(*expvar.Int)
	}
	return expvar.NewInt(n)
}
func (p *Pool) IsDown() bool { p.mu.Lock(); d := p.down; p.mu.Unlock(); return d }
func (p *Pool) Enqueue(ctx context.Context, j *domain.Job) error {
	p.mu.Lock()
	if p.down {
		p.mu.Unlock()
		return domain.ErrShuttingDown
	}
	if len(p.hq)+len(p.nq) >= p.cap {
		p.mu.Unlock()
		return domain.ErrQueueFull
	}
	p.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if j.Priority == domain.High {
		p.hq <- j
	} else {
		p.nq <- j
	}
	p.qh.Set(int64(len(p.hq)))
	p.qn.Set(int64(len(p.nq)))
	p.created.Add(1)
	log.Printf("job created id=%s", j.ID)
	return nil
}
func (p *Pool) Resize(n int) error {
	if n <= 0 {
		return errors.New("workers must be >0")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	c := len(p.workers)
	for ; c < n; c++ {
		ch := make(chan struct{})
		p.workers[p.wid] = ch
		p.wid++
		p.wg.Add(1)
		go p.worker(ch)
	}
	for ; c > n; c-- {
		for id, ch := range p.workers {
			close(ch)
			delete(p.workers, id)
			break
		}
	}
	return nil
}
func (p *Pool) next(stop <-chan struct{}) (*domain.Job, bool) {
	for {
		select {
		case <-p.ctx.Done():
			return nil, false
		case <-stop:
			return nil, false
		case j := <-p.hq:
			return j, true
		default:
		}
		select {
		case <-p.ctx.Done():
			return nil, false
		case <-stop:
			return nil, false
		case j := <-p.hq:
			return j, true
		case j := <-p.nq:
			return j, true
		}
	}
}
func (p *Pool) worker(stop <-chan struct{}) {
	defer p.wg.Done()
	for {
		j, ok := p.next(stop)
		if !ok {
			return
		}
		p.exec(j)
	}
}
func (p *Pool) exec(j *domain.Job) {
	n := time.Now().UTC()
	j.Status = domain.Running
	if j.StartedAt == nil {
		j.StartedAt = &n
	}
	p.repo.Save(j)
	p.run.Add(1)
	p.mu.Lock()
	p.active[j.ID] = struct{}{}
	p.mu.Unlock()
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
		p.fail.Add(1)
	} else {
		p.succ.Add(1)
	}
	f := time.Now().UTC()
	j.FinishedAt = &f
	p.repo.Save(j)
	p.run.Add(-1)
	p.mu.Lock()
	delete(p.active, j.ID)
	p.mu.Unlock()
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
	p.mu.Unlock()
	cancel := func(ch chan *domain.Job) {
		for {
			select {
			case j := <-ch:
				j.Status = domain.Canceled
				f := time.Now().UTC()
				j.FinishedAt = &f
				j.LastError = "canceled on shutdown"
				p.repo.Save(j)
			default:
				return
			}
		}
	}
	cancel(p.hq)
	cancel(p.nq)
	p.qh.Set(0)
	p.qn.Set(0)
	d := make(chan struct{})
	go func() { p.cancel(); p.wg.Wait(); close(d) }()
	select {
	case <-d:
		return nil, nil
	case <-ctx.Done():
		p.mu.Lock()
		ids := make([]string, 0, len(p.active))
		for id := range p.active {
			ids = append(ids, id)
		}
		p.mu.Unlock()
		return ids, ctx.Err()
	}
}
