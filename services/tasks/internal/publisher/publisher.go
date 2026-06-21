package publisher

import (
	"context"
	"encoding/json"
	"time"

	amqpsetup "example.com/pz14-rabbit-jobs/services/tasks/internal/amqp"
	"example.com/pz14-rabbit-jobs/services/tasks/internal/jobs"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	ch *amqp.Channel
}

func New(ch *amqp.Channel) *Publisher {
	return &Publisher{ch: ch}
}

func (p *Publisher) PublishJob(job jobs.TaskJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return p.ch.PublishWithContext(
		ctx,
		"",
		amqpsetup.JobsQueue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}
