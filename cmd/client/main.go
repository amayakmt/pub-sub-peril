package main

import (
	"fmt"

	gamelogic "github.com/amayakmt/pub-sub-peril/internal/gamelogic"
	pubsub "github.com/amayakmt/pub-sub-peril/internal/pubsub"
	routing "github.com/amayakmt/pub-sub-peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// connect to RabbitMQ
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ successfully")

	// welcome the client to the game
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to welcome client:", err)
		return
	}
	// declare and bind the "pause" queue to the direct exchange
	// exchange: peril_direct from a constant in the internal/routing package
	// queueName: pause.username where `username` is the user's input.
	//            the `pause` section of the name is the routing key constant in the
	//            internal/routing package and is joined by a `.`
	// routing key: pause (this is a constant in the `internal/routing` package)
	// queueType: transient
	ch, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%s", userName),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
	)
	if err != nil {
		fmt.Println("Failed to declare and bind the pause queue:", err)
		return
	}
	defer ch.Close()

	gs := gamelogic.NewGameState(userName)

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			fmt.Println("Spawning units...")
			err := gs.CommandSpawn(input)
			if err != nil {
				fmt.Println("Failed to spawn units:", err)
			}

		case "move":
			fmt.Println("Moving units...")
			_, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println("Failed to move units:", err)
			}

		case "status":
			fmt.Println("Checking status...")
			gs.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}
}
