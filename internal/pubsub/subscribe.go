package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	// call DeclareAndBind to ensure that the given queue exists and is bound to the exchange
	_, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	// get a new channel of amqp.Delivery structs by using the channel.Consume method
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range msgs {
			var msg T
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Println("NackDiscard: failed to unmarshal message:", err)
				d.Nack(false, false)
				continue
			}
			switch handler(msg) {
			case Ack:
				log.Println("Ack")
				d.Ack(false)
			case NackRequeue:
				log.Println("NackRequeue")
				d.Nack(false, true)
			case NackDiscard:
				log.Println("NackDiscard")
				d.Nack(false, false)
			}
		}
	}()
	return nil
}
