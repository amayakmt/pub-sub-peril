package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/amayakmt/pub-sub-peril/internal/gamelogic"
	"github.com/amayakmt/pub-sub-peril/internal/pubsub"
	"github.com/amayakmt/pub-sub-peril/internal/routing"
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

	// channel used for publishing
	publishCh, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to create channel:", err)
		return
	}
	defer publishCh.Close()

	// welcome the client to the game
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to welcome client:", err)
		return
	}

	// create game state
	gs := gamelogic.NewGameState(userName)

	// declare and bind the "pause" queue to the direct exchange
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, userName),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
		handlerPause(gs),
	)
	if err != nil {
		fmt.Println("Failed to declare and bind the pause queue:", err)
		return
	}

	// declare and bind the "move" queue to the direct exchange
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("move.%s", userName),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.QueueTypeTransient,
		handlerMove(gs, publishCh),
	)
	if err != nil {
		fmt.Println("Failed to declare and bind the move queue:", err)
		return
	}

	// declare and bind the "war" queue to the direct exchange
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.QueueTypeDurable,
		handlerWar(gs, publishCh),
	)
	if err != nil {
		fmt.Println("Failed to declare and bind the war queue:", err)
		return
	}

	// main game loop
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
			mv, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println("Failed to move units:", err)
				continue
			}
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName),
				mv,
			)
			if err != nil {
				fmt.Println("Failed to publish move:", err)
				continue
			}
			fmt.Println("Move published successfully")

		case "status":
			fmt.Println("Checking status...")
			gs.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			if len(input) < 2 {
				fmt.Println("Usage: spam <number>")
				return
			}
			num, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println("Invalid number:", input[1])
				return
			}
			for i := 0; i < num; i++ {
				msg := gamelogic.GetMaliciousLog()
				err = pubsub.PublishGob(
					publishCh,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.GameLogSlug, userName),
					routing.GameLog{
						CurrentTime: time.Now(),
						Message:     msg,
						Username:    userName,
					},
				)
			}

		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		switch gs.HandleMove(move) {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Println("Failed to publish war recognition:", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			fmt.Println(msg)
			return PublishGameLog(ch, gs.GetUsername(), msg)
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			fmt.Println(msg)
			return PublishGameLog(ch, gs.GetUsername(), msg)
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			fmt.Println(msg)
			return PublishGameLog(ch, gs.GetUsername(), msg)
		default:
			fmt.Printf("Unknown war outcome: %v", outcome)
			return pubsub.NackDiscard
		}
	}
}

func PublishGameLog(ch *amqp.Channel, userName, message string) pubsub.AckType {
	err := pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, userName),
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     message,
			Username:    userName,
		},
	)
	if err != nil {
		fmt.Println("Failed to publish game log:", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
