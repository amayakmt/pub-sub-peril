package main

import (
	"fmt"
	"os"
	"os/signal"

	pubsub "github.com/amayakmt/pub-sub-peril/internal/pubsub"
	routing "github.com/amayakmt/pub-sub-peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Start a connection to RabbitMQ
	fmt.Println("Starting Peril server...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ successfully")

	// Create a new channel
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to create channel:", err)
		return
	}
	defer ch.Close()

	// Publish message to the exchange
	err = pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	)
	if err != nil {
		fmt.Println("Failed to publish message:", err)
		return
	}

	// Wait for Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	// Print message and close connection
	fmt.Println("\nShutting down...")
	conn.Close()
	fmt.Println("Connection closed.")
}
