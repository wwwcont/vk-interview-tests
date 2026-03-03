package main

import (
	"log"
	"time"

	limapp "vk-interview-tests/subprojects/task3_rate_limiter/app"
	server "vk-interview-tests/subprojects/task3_rate_limiter_server"
)

const Addr = ":8083"

func main() {
	lim := limapp.New(10, 20, 10*time.Minute)
	log.Fatal(server.Run(Addr, lim))
}
