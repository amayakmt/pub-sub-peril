package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	// call DeclareAndBind to ensure that the given queue exists and is bound to the exchange
	_, _, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}
	// get a new channel of amqp.Delivery structs by using the channel.Consume method
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(
		queueName,
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
	go func() {
		for d := range msgs {
			var msg T
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				// log the error and nack the message
				d.Nack(false, false)
				continue
			}
			handler(msg)
			d.Ack(false)
		}
	}()

	return nil
}
