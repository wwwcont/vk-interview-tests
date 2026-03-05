// Binary server starts the demo HTTP API around the circuit breaker.
//
// Very simple flow:
// 1) create breaker with default config,
// 2) compose HTTP handler,
// 3) listen on :8080.
package main

import (
	"log"
	"net/http"
	"time"

	"vk-interview-tests/internal/domain/breaker"
	"vk-interview-tests/internal/transport/httpapi"
)

// main is a tiny composition root for the demo service.
func main() {
	b, err := breaker.New(breaker.Config{MaxFailures: 3, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", httpapi.NewHandler(b, httpapi.SleepCaller{})))
}
