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
- Go installed (version 1.18+ recommended)

### Steps

```bash
# Clone the repository
git clone https://github.com/yourusername/three-phase-commit.git
cd three-phase-commit

# Start servers (open separate terminals)

# Coordinator
go run main.go --role=coordinator --port=8000

# Participant 1
go run main.go --role=participant --id=1 --port=8001

# Participant 2
go run main.go --role=participant --id=2 --port=8002

# Run a client transaction (in another terminal)
go run client.go
