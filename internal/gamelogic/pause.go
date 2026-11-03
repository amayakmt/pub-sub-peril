package gamelogic

import (
	"fmt"

	"github.com/amayakmt/pub-sub-peril/internal/routing"
)

func (gs *GameState) HandlePause(ps routing.PlayingState) {
	defer fmt.Println("------------------------")
	fmt.Println()
	if ps.IsPaused {
		fmt.Println("==== Pause Detected ====")
		gs.pauseGame()
	} else {
		fmt.Println("==== Resume Detected ====")
		gs.resumeGame()
	}
}
