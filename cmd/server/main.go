package main

import (
	"fmt"

	gamelogic "github.com/amayakmt/pub-sub-peril/internal/gamelogic"
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

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.QueueTypeDurable,
	)
	if err != nil {
		fmt.Println("Failed to declare and bind the game log queue:", err)
		return
	}

	// print the help as the server starts to see the commands the user of the REPL can use.
	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Sending a pause message...")
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
			fmt.Println("Pause message sent successfully.")

		case "resume":
			fmt.Println("Sending a resume message...")
			// Publish message to the exchange
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
				return
			}
			fmt.Println("Resume message sent successfully.")

		case "quit":
			// Quit the server
			fmt.Println("Shutting down...")
			conn.Close()
			fmt.Println("Connection closed.")
			return

		default:
			fmt.Println("Unknown command. Type 'pause', 'resume', or 'quit'.")
		}
	}
}
