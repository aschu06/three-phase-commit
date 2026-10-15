# Three-Phase Commit Transaction System

## Description
This project implements a distributed transaction system using the Three-Phase Commit protocol. The system coordinates transactions across multiple servers while maintaining consistency during commit and abort decisions.

## Features
- Supports distributed transactions across multiple servers
- Implements GET and PUT operations within transactions
- Uses locking to prevent conflicting operations
- Logs transaction operations and state changes
- Handles commit and abort decisions using the 3PC protocol
- Uses mutexes for thread-safe access to shared server state

## Tech Used
- Go
- Distributed systems concepts
- Concurrency
- Mutexes
- Transaction logging

## How to Run

### Prerequisites
- Go 1.22 or later

### Steps

1. Clone the repository:
```bash
git clone https://github.com/aschu06/three-phase-commit.git
cd three-phase-commit
```

2. Run the test suite with the race detector:
```bash
go test -race
```

To run a single test, for example coordinator recovery:
```bash
go test -race -run TestRestartCommit
```

# Run a client transaction (in another terminal)
go run client.go
