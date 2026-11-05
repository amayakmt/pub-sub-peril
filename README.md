# Pub/Sub Peril

The RabbitMQ messaging layer for a multiplayer strategy game, written in Go. Clients publish moves, wars and game logs. A server pauses the game and persists the log. Nobody talks to anyone directly.

Built on the [Boot.dev `learn-pub-sub-starter`](https://github.com/bootdotdev/learn-pub-sub-starter) project. The game rules came with the starter. The messaging code is mine.

![Two Peril clients and the server running against one RabbitMQ broker: alice moves into bob's territory, a war is resolved, the game is paused, and the server writes the log](docs/demo.png)

*Real output from a local run: two clients and one server on one broker. Output trimmed to the relevant lines.*

## Motivation

I wanted hands-on practice with message-driven design, and this course project is a structured way to get it. The starter ships the game rules and two empty `main` functions. Everything that touches RabbitMQ is what I implemented:

- Generic publish and subscribe helpers (`PublishJSON[T]`, `SubscribeJSON[T]`, `SubscribeGob[T]`) so handlers receive typed values, not raw bytes.
- Explicit acknowledgement. Every handler returns `Ack`, `NackRequeue` or `NackDiscard`, and the subscriber logs which one it chose.
- Durable and transient queues, a dead-letter exchange on every queue, and a prefetch limit of 10 per consumer.
- Two wire formats: JSON for game events, Gob for logs.
- The server and client REPLs and their handlers, which wire the game rules to those queues.

## What is mine and what is not

| Part | Source |
|---|---|
| `internal/pubsub/` (publish, queue declaration, subscribe, ack types) | Written by me |
| `cmd/server`, `cmd/client` (queue topology, handlers, REPLs) | Written by me |
| Dead-letter exchange constant, `./rabbit.sh setup` | Added by me |
| `internal/gamelogic/` (units, moves, war resolution, game state, log file) | Provided by the starter, unchanged apart from import paths |
| `internal/routing/` message models and most names | Provided by the starter |
| `rabbit.sh`, `multiserver.sh` | Provided by the starter |

## How it works

```mermaid
flowchart LR
    S[server] -- "pause / resume (JSON)" --> D{{peril_direct}}
    D --> P["pause.&lt;user&gt; (transient)"]
    C1[client] -- "army_moves.&lt;user&gt; (JSON)" --> T{{peril_topic}}
    T --> M["move.&lt;user&gt; (transient)"]
    M -- "units overlap" --> T2["war.&lt;user&gt;"]
    T2 --> W["war (durable, shared)"]
    W -- "game_logs.&lt;user&gt; (Gob)" --> L["game_logs (durable)"]
    L --> S
```

| Queue | Exchange | Binding key | Lifetime | Consumer |
|---|---|---|---|---|
| `pause.<user>` | `peril_direct` | `pause` | transient | each client |
| `move.<user>` | `peril_topic` | `army_moves.*` | transient | each client |
| `war` | `peril_topic` | `war.*` | durable, shared | all clients |
| `game_logs` | `peril_topic` | `game_logs.*` | durable | server |

Every queue is declared with `x-dead-letter-exchange: peril_dlx`.

Acknowledgement rules:

| Situation | Result |
|---|---|
| Your own move arrives back | `NackDiscard` |
| Another player's move, no overlap | `Ack` |
| Another player's move lands on your units | `Ack`, then a war message is published |
| War message you are not part of | `NackRequeue`, so the other client can take it |
| War message with no overlapping units | `NackDiscard` |
| Message that fails to decode | `NackDiscard` |

## Quick Start

Requirements: Go 1.27+ and Docker (or any RabbitMQ 3.x with AMQP on `localhost:5672`, guest/guest).

```bash
git clone https://github.com/amayakmt/pub-sub-peril
cd pub-sub-peril

./rabbit.sh start     # RabbitMQ 3.13 + management UI on :15672
./rabbit.sh setup     # create the three exchanges (see note below)

go run ./cmd/server   # terminal 1
go run ./cmd/client   # terminal 2, enter a username
go run ./cmd/client   # terminal 3, enter another username
```

The code does not declare exchanges. Without `./rabbit.sh setup` the server exits with `NOT_FOUND - no exchange 'peril_topic'`. The script creates `peril_direct` (direct), `peril_topic` (topic) and `peril_dlx` (fanout).

## Usage

Client commands:

| Command | Effect |
|---|---|
| `spawn <location> <rank>` | Add a unit. Ranks: `infantry` (1 power), `cavalry` (5), `artillery` (10) |
| `move <location> <id> [<id>...]` | Move units and publish the move |
| `status` | Show your units and whether the game is paused |
| `spam <n>` | Publish `n` random game-log messages |
| `help`, `quit` | |

Locations: `americas`, `europe`, `africa`, `asia`, `australia`, `antarctica`.

Server commands: `pause`, `resume`, `help`, `quit`.

A war starts when you move into a location where another player has units. The side with the higher total power wins, and the winner is written to `game.log` by the server. Moves are rejected while the game is paused.

Run several servers at once to watch competing consumers share the `game_logs` queue:

```bash
./multiserver.sh 3
```

Open `http://localhost:15672` (guest/guest) to watch queues, bindings and message rates while you play.

## Project layout

```
cmd/server/          pause/resume REPL, game_logs consumer
cmd/client/          game REPL, pause/move/war consumers
internal/pubsub/     publish and subscribe helpers, queue declaration, ack types
internal/routing/    exchange, queue and key names, message models
internal/gamelogic/  units, moves, war resolution, game state (from the starter)
rabbit.sh            start, setup, stop, logs
multiserver.sh       run N servers (from the starter)
```

## Known limitations

- Credentials and host are hardcoded to `amqp://guest:guest@localhost:5672/`.
- The defender's units are not removed when the attacker wins. Only a losing attacker loses units. This is in the starter's `gamelogic/war.go`, which I have not changed.
- `spam` with a missing or invalid count exits the client.
- `peril_dlx` has no queue bound to it, so dead-lettered messages are dropped. Bind a queue to inspect them.
- No tests.

## Contributing

Issues and pull requests are welcome. Run `go vet ./...` and `go build ./...` before opening a PR. Keep new queue names and routing keys in `internal/routing`, and have every handler return an explicit `AckType`.

## Credits

Game rules, message models and the project scaffold come from the Boot.dev "Learn Pub/Sub" course and its [starter repo](https://github.com/bootdotdev/learn-pub-sub-starter).