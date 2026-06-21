package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"example.com/pz14-rabbit-jobs/services/worker/internal/jobs"
	"example.com/pz14-rabbit-jobs/services/worker/internal/rabbitsetup"
	"example.com/pz14-rabbit-jobs/services/worker/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

const maxAttempts = 3

type Consumer struct {
	rabbitURL string
	processed *store.ProcessedStore
}

func New(rabbitURL string) *Consumer {
	return &Consumer{
		rabbitURL: rabbitURL,
		processed: store.NewProcessedStore(),
	}
}

func (c *Consumer) Run() error {
	conn, err := amqp.Dial(c.rabbitURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := rabbitsetup.DeclareQueues(ch); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}

	msgs, err := ch.Consume(
		rabbitsetup.JobsQueue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	log.Printf("worker started, queue=%s, max_attempts=%d", rabbitsetup.JobsQueue, maxAttempts)
	for d := range msgs {
		c.handleDelivery(ch, d)
	}
	return nil
}

func (c *Consumer) handleDelivery(ch *amqp.Channel, d amqp.Delivery) {
	var job jobs.TaskJob
	if err := json.Unmarshal(d.Body, &job); err != nil {
		log.Printf("bad message: %v", err)
		_ = d.Nack(false, false)
		return
	}

	if c.processed.Exists(job.MessageID) {
		log.Printf("message already processed: message_id=%s", job.MessageID)
		_ = d.Ack(false)
		return
	}

	if err := processTask(job); err != nil {
		log.Printf("job failed: task_id=%s attempt=%d error=%v", job.TaskID, job.Attempt, err)
		job.Attempt++
		if job.Attempt <= maxAttempts {
			if err := publishJob(ch, rabbitsetup.JobsQueue, job); err != nil {
				log.Printf("retry publish error: %v", err)
			}
			_ = d.Ack(false)
			return
		}
		if err := publishJob(ch, rabbitsetup.DLQQueue, job); err != nil {
			log.Printf("dlq publish error: %v", err)
		}
		_ = d.Ack(false)
		return
	}

	c.processed.MarkDone(job.MessageID)
	log.Printf("job processed: task_id=%s message_id=%s attempt=%d", job.TaskID, job.MessageID, job.Attempt)
	_ = d.Ack(false)
}

func processTask(job jobs.TaskJob) error {
	time.Sleep(2 * time.Second)
	if job.TaskID == "t_fail" {
		return fmt.Errorf("simulated processing error")
	}
	return nil
}

func publishJob(ch *amqp.Channel, queue string, job jobs.TaskJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ch.PublishWithContext(
		ctx,
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}
