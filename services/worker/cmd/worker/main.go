package main

import (
	"log"
	"os"

	"example.com/pz14-rabbit-jobs/services/worker/internal/consumer"
)

func main() {
	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	c := consumer.New(rabbitURL)
	if err := c.Run(); err != nil {
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
