package main

import (
	"log"
	"net/http"
	"os"

	"example.com/pz14-rabbit-jobs/services/tasks/internal/amqp"
	httpapi "example.com/pz14-rabbit-jobs/services/tasks/internal/http"
	"example.com/pz14-rabbit-jobs/services/tasks/internal/publisher"
)

func main() {
	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	addr := getenv("TASKS_ADDR", ":8082")

	conn, err := amqp.Connect(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit connect error: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("rabbit channel error: %v", err)
	}
	defer ch.Close()

	if err := amqp.DeclareQueues(ch); err != nil {
		log.Fatalf("declare queues error: %v", err)
	}

	pub := publisher.New(ch)
	handler := httpapi.NewHandler(pub)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handler.Health)
	mux.HandleFunc("/v1/jobs/process-task", handler.ProcessTask)

	log.Printf("tasks service started on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
