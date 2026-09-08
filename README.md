# herdr-scribe

Agent runtime telemetry for Herdr exported over OpenTelemetry, without ever
transmitting what the agents are actually doing.

## Status

Implemented so far:

- **1.1 — Minimal socket connection + `ping`.** A reusable, one-shot
  newline-delimited JSON RPC client over the Herdr Unix socket, exercised by a
  tiny `ping` demo binary.
- **1.5 — Subscription on its own dedicated connection.** A `Subscriber` that
  opens a fresh connection exclusively for `events.subscribe`, owns it with a
  dedicated reader task, and logs every pushed event verbatim. The connection
  is write-once by design: RPCs must open their own short-lived connection
  (`client.Call`) and can never reuse this one.

## Requirements

- Go 1.26+
- A running Herdr instance exposing its Unix socket

## Build

```sh
go build -o herdr-scribe .
```

## Run

The binary sends `ping` over Herdr's Unix socket and confirms `pong`:

```sh
./herdr-scribe
```

The socket path is read from `HERDR_SOCKET_PATH` and NEEDS to be set in order to run correctly

```sh
HERDR_SOCKET_PATH=/path/to/your/herdr.sock ./herdr-scribe
```

## Test

```sh
go test ./...
```

Tests run against a stub Unix socket server and do not require a live Herdr
instance.

## Layout

```
main.go                      # ping + events.subscribe demo binary
internal/client/             # low-level NDJSON RPC client over the Unix socket
  client.go                  # one-shot Call helper, Dial, socket path resolution
  rpc.go                     # wire types, frame encode/decode
  client_test.go             # framing + end-to-end tests against a stub server
internal/events/             # scoped event subscription payload + long-lived subscriber
  subscription.go            # event names, BuildParams subscription payload
  subscription_test.go       # BuildParams tests
  subscriber.go              # dedicated-connection Subscriber with verbatim event logging
  subscriber_test.go         # subscriber tests against a stub server
PLAN.md                      # full multi-phase implementation plan
```

## License

MIT — see [`LICENSE`](LICENSE).