package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"vk-interview-tests/internal/app"
	"vk-interview-tests/internal/infra"
	httptransport "vk-interview-tests/internal/transport/http"
)

func main() {
	w := flag.Int("workers", env("WORKERS", 4), "initial workers")
	q := flag.Int("queue-capacity", env("QUEUE_CAPACITY", 1000), "queue capacity")
	flag.Parse()
	repo := infra.NewRepo()
	pool := infra.NewPool(repo, *w, *q)
	svc := app.New(repo, pool)
	srv := &http.Server{Addr: ":8080", Handler: httptransport.New(svc).Routes()}
	go func() {
		log.Printf("listen %s workers=%d", srv.Addr, *w)
		if e := srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Fatal(e)
		}
	}()
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
	ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_, _ = svc.Shutdown(ctx, 10*time.Second)
	_ = srv.Shutdown(ctx)
}
func env(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return d
}
