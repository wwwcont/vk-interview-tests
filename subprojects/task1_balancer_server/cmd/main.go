package main

import (
	"context"
	"log"

	balapp "vk-interview-tests/subprojects/task1_balancer/app"
	"vk-interview-tests/subprojects/task1_balancer/domain"
	server "vk-interview-tests/subprojects/task1_balancer_server"
)

const Addr = ":8081"

type backend struct{ id string }

func (b backend) ID() string    { return b.id }
func (b backend) Healthy() bool { return true }
func (b backend) Do(context.Context, domain.Request) (domain.Response, error) {
	return domain.Response{}, nil
}

func main() {
	svc := balapp.New([]domain.Backend{backend{"b1"}, backend{"b2"}})
	log.Fatal(server.Run(Addr, svc))
}
