package main

import (
	"log"
	"time"

	cacheapp "vk-interview-tests/subprojects/task5_ttl_cache/app"
	"vk-interview-tests/subprojects/task5_ttl_cache/domain"
	server "vk-interview-tests/subprojects/task5_ttl_cache_server"
)

const Addr = ":8085"

func main() {
	cache := cacheapp.New(domain.Config{MaxEntries: 100, StaleGrace: 5 * time.Second})
	log.Fatal(server.Run(Addr, cache))
}
