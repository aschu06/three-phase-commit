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

## What I Learned
- How distributed transactions are coordinated
- Why locking is needed for consistency
- How commit protocols handle failure cases
- How to manage shared state safely in Go
