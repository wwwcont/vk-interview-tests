package main

import (
	"log"

	poolapp "vk-interview-tests/subprojects/task2_worker_pool/app"
	server "vk-interview-tests/subprojects/task2_worker_pool_server"
)

const Addr = ":8082"

func main() {
	pool := poolapp.New(4)
	log.Fatal(server.Run(Addr, pool))
}
