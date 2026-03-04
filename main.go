package main

import (
	"errors"
	"log"
	"net/http"

	"vk-interview-tests/internal/domain"
	"vk-interview-tests/internal/httpapi"
	"vk-interview-tests/internal/service"
)

func main() {
	svc := service.New(domain.NewBalancer())
	log.Printf("listening on :8080")
	if err := http.ListenAndServe(":8080", httpapi.NewMux(svc)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
