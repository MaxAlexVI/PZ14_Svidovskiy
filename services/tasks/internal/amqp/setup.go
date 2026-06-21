package amqp

import rabbit "github.com/rabbitmq/amqp091-go"

const (
	JobsQueue = "task_jobs"
	DLQQueue  = "task_jobs_dlq"
)

func Connect(url string) (*rabbit.Connection, error) {
	return rabbit.Dial(url)
}

func DeclareQueues(ch *rabbit.Channel) error {
	_, err := ch.QueueDeclare(
		DLQQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	args := rabbit.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": DLQQueue,
	}
	_, err = ch.QueueDeclare(
		JobsQueue,
		true,
		false,
		false,
		false,
		args,
	)
	return err
}
