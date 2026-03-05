// Пакет main поднимает демо HTTP API вокруг circuit breaker.
//
// Логика очень простая:
// 1) создаём брекер с дефолтным конфигом,
// 2) собираем HTTP-обработчики,
// 3) слушаем порт :8080.
package main

import (
	"log"
	"net/http"
	"time"

	"vk-interview-tests/internal/domain/breaker"
	"vk-interview-tests/internal/transport/httpapi"
)

// main — минимальная точка сборки приложения.
func main() {
	b, err := breaker.New(breaker.Config{MaxFailures: 3, ResetTimeout: time.Second, HalfOpenMaxProbes: 1})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", httpapi.NewHandler(b, httpapi.SleepCaller{})))
}
