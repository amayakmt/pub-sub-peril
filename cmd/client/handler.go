package main

import (
	"fmt"

	gamelogic "github.com/amayakmt/pub-sub-peril/internal/gamelogic"
	routing "github.com/amayakmt/pub-sub-peril/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(state)
	}
}
