// В этом файле — короткие тесты на основную логику брекера.
//
// Идея простая: оставить минимальный набор,
// который можно быстро написать в конце лайвкодинга,
// но при этом проверить ключевые сценарии:
// - открытие после серии ошибок,
// - отклонение вызовов в OPEN,
// - ленивый переход в HALF_OPEN и закрытие на успешной пробе,
// - дефолтные правила классификации ошибок,
// - лимит проб в HALF_OPEN.
package breaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newBreaker создаёт брекер для теста.
// Если конфиг некорректный, тест сразу падает.
func newBreaker(t *testing.T, cfg Config) *CircuitBreaker {
	t.Helper()
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeClock подменяет источник времени,
// чтобы тесты были детерминированными и без sleep.
func fakeClock(b *CircuitBreaker, start time.Time) func(time.Duration) {
	now := start
	b.now = func() time.Time { return now }
	return func(d time.Duration) { now = now.Add(d) }
}

// toHalfOpen переводит брекер в HALF_OPEN через 2 шага:
// 1) сначала открываем брекер ошибкой,
// 2) двигаем время на timeout и проверяем состояние.
func toHalfOpen(t *testing.T, b *CircuitBreaker, step func(time.Duration), timeout time.Duration) {
	t.Helper()
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	step(timeout)
	if got := b.State().State; got != StateHalfOpen {
		t.Fatalf("state=%s want HALF_OPEN", got)
	}
}

// TestClosedToOpenAtThreshold проверяет открытие при достижении лимита ошибок.
func TestClosedToOpenAtThreshold(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 2, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	if got := b.State(); got.State != StateClosed || got.Failures != 1 {
		t.Fatalf("unexpected state: %+v", got)
	}
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	if got := b.State().State; got != StateOpen {
		t.Fatalf("state=%s want OPEN", got)
	}
}

// TestOpenRejectsAndSkipsFn проверяет, что в OPEN вызов отклоняется,
// а защищаемая функция вообще не запускается.
func TestOpenRejectsAndSkipsFn(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	_ = b.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })
	called := false
	err := b.Execute(context.Background(), func(context.Context) error { called = true; return nil })
	if !errors.Is(err, ErrBreakerOpen) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}

// TestLazyHalfOpenAndProbeSuccessCloses проверяет,
// что после таймаута брекер лениво становится HALF_OPEN,
// и успешная проба возвращает его в CLOSED.
func TestLazyHalfOpenAndProbeSuccessCloses(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	step := fakeClock(b, time.Unix(10, 0))
	toHalfOpen(t, b, step, time.Second)
	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := b.State(); got.State != StateClosed || got.Failures != 0 {
		t.Fatalf("unexpected state: %+v", got)
	}
}

// TestClassifierRules проверяет дефолтную классификацию ошибок:
// - context.Canceled не должен ломать брекер,
// - context.DeadlineExceeded должен считаться failure.
func TestClassifierRules(t *testing.T) {
	t.Run("context canceled is not failure", func(t *testing.T) {
		b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
		err := b.Execute(context.Background(), func(context.Context) error { return context.Canceled })
		if !errors.Is(err, context.Canceled) || b.State().State != StateClosed {
			t.Fatalf("err=%v state=%s", err, b.State().State)
		}
	})

	t.Run("deadline exceeded is failure", func(t *testing.T) {
		b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
		_ = b.Execute(context.Background(), func(context.Context) error { return context.DeadlineExceeded })
		if got := b.State().State; got != StateOpen {
			t.Fatalf("state=%s want OPEN", got)
		}
	})
}

// TestHalfOpenNonFailureAndProbeLimit проверяет два важных кейса HALF_OPEN:
// 1) нейтральная ошибка (context.Canceled) не должна заново открывать брекер,
// 2) если все слоты проб заняты, должна вернуться ErrTooManyProbes.
func TestHalfOpenNonFailureAndProbeLimit(t *testing.T) {
	b := newBreaker(t, Config{MaxFailures: 1, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	step := fakeClock(b, time.Unix(10, 0))
	toHalfOpen(t, b, step, time.Second)

	if err := b.Execute(context.Background(), func(context.Context) error { return context.Canceled }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if got := b.State(); got.State != StateHalfOpen || got.Failures != 1 {
		t.Fatalf("unexpected after non-failure: %+v", got)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = b.Execute(context.Background(), func(context.Context) error { close(started); <-release; return nil })
	}()
	<-started
	if err := b.Execute(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrTooManyProbes) {
		t.Fatalf("err=%v want ErrTooManyProbes", err)
	}
	close(release)
}
