# Queue Service

A simple HTTP FIFO queue broker written in Go without third-party dependencies.

## Run

```bash
go run main.go 8080
```

The port is passed as the first command-line argument.

## Test

```bash
go test main.go main_test.go
```

## Usage

Add a message to a queue:

```bash
curl -XPUT 'http://127.0.0.1:8080/pet?v=cat'
curl -XPUT 'http://127.0.0.1:8080/pet?v=dog'
```

Get a message from a queue:

```bash
curl -w '\n' 'http://127.0.0.1:8080/pet'
```

The `-w '\n'` option only adds a line break to the terminal output; the service
returns the message body unchanged.

If the queue is empty, the service returns `404`.

Wait up to `N` seconds for a message:

```bash
curl 'http://127.0.0.1:8080/pet?timeout=10'
```

Waiting consumers are served in the same order as their `GET` requests arrive.
