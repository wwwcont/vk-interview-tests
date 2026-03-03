package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vk-interview-tests/internal/domain/counter"
	"vk-interview-tests/internal/infrastructure/repo/memory"
	"vk-interview-tests/internal/interfaces/httpapi"
)

// main поднимает очень простой runnable-сервер для демо и ручной проверки.
func main() {
	repo := memory.New()

	svc, err := counter.New(repo, counter.Config{
		Shards:        32,
		FlushInterval: 2 * time.Second,
		FlushTimeout:  1 * time.Second,
	})
	if err != nil {
		log.Fatalf("init service: %v", err)
	}
	defer func() {
		if err := svc.Close(); err != nil {
			log.Printf("close service: %v", err)
		}
	}()

	h := httpapi.New(svc)
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: h.Handler(),
	}

	go func() {
		log.Printf("listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	waitForSignal()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
}

// waitForSignal блокируется до SIGINT/SIGTERM, чтобы корректно завершить процесс.
func waitForSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
