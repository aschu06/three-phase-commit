package commit

// ------------------------------------------
//                  COMMON
// ------------------------------------------

type TransactionState int

const (
	stateOperations TransactionState = iota
	stateVotedNo
	stateVotedYes
	statePreCommitted
	stateAborted
	stateCommitted
)

// Common args struct because RPCs generally have the transaction ID as their only argument
type RPCArgs struct {
	Tid int
}

type PrepareReply struct {
	// Your fields here
	VotedFor bool
	Relevant bool
}

type QueryReply struct {
	// Your fields here
	States map[int]TransactionState //Servers should respond with the state of each transaction that it knows about. so needs transaction ID -> state
}

type CommitReply struct {
	// Your fields here
	Results map[string]interface{} //Each server performs its operations… and replies with the values of any Get operations. needs the results of the GETS
}
