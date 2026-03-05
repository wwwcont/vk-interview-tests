package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	b, err := NewCircuitBreaker(Config{MaxFailures: 3, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", newHTTPHandler(b, sleepCaller{})))
}
