package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	QueueTypeDurable   SimpleQueueType = "durable"
	QueueTypeTransient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	// create a new channel from the connection
	// this channel will be used to declare and bind the queue
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// declare the queue with the appropriate durability settings
	isDurable := queueType == QueueTypeDurable
	queue, err := ch.QueueDeclare(
		queueName,
		isDurable,
		!isDurable,
		!isDurable,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// bind the queue to the exchange with the specified routing key
	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	// return the channel and queue if successful
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, queue, nil
}
