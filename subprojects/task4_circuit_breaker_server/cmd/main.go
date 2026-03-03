package main

import (
	"log"
	"time"

	brapp "vk-interview-tests/subprojects/task4_circuit_breaker/app"
	"vk-interview-tests/subprojects/task4_circuit_breaker/domain"
	server "vk-interview-tests/subprojects/task4_circuit_breaker_server"
)

const Addr = ":8084"

func main() {
	br := brapp.New(domain.Config{WindowSize: 10, MinRequestsToTrip: 5, ErrorThreshold: 0.5, ResetTimeout: 5 * time.Second, MaxProbe: 1})
	log.Fatal(server.Run(Addr, br))
}
