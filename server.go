package commit

import (
	"sync"
)

// for i := 0; i < len(sv.transactions[args.Tid].locks); i++ {
// 	key := sv.transactions[args.Tid].locks[i]
// 	sv.store[key].lock.Unlock()
// }

// sv.transactions[args.Tid].locks = nil

type StoreItem struct {
	value interface{}
	lock  sync.RWMutex
	// Any extra fields here
}

type Operation struct { //server stores list of operations per transaction
	key   string
	value interface{}
	op    bool //true is get, false is set
}
type record struct { //keep track of each transaction on a server
	ops    []Operation
	state  TransactionState
	locks  []Operation
	output map[string]interface{}
}
type Server struct {
	mu    sync.Mutex
	store map[string]*StoreItem
	// Your fields here
	transactions map[int]*record
}

// Prepare handler
//
// This function should:
// 1. Attempt to obtain locks for the given transaction
// 2. If this succeeds, vote Yes
// 3. If this fails, release any obtained locks and vote No
func (sv *Server) Prepare(args *RPCArgs, reply *PrepareReply) {
	// Your code here
	sv.mu.Lock()

	tid := args.Tid
	if sv.transactions[tid] == nil { // If a server receives Prepare for a transaction that it does not have any operations logged for, it replies that the transaction isn't relevant to it.
		reply.VotedFor = true //not relevant, so don't block the transaction
		reply.Relevant = false
		sv.mu.Unlock()
		return
	} else {
		reply.Relevant = true
	}

	for i := 0; i < len(sv.transactions[tid].ops); i++ { //go through all operations for keys
		if sv.transactions[tid].ops[i].op == true { //get operation, need tryRlock
			if sv.store[sv.transactions[tid].ops[i].key].lock.TryRLock() == true { //if lock is successful
				sv.transactions[tid].locks = append(sv.transactions[tid].locks, sv.transactions[tid].ops[i])
			} else { //lock is unsuccessful, need to go through all transaciton locks and unlock
				for x := 0; x < len(sv.transactions[tid].locks); x++ {
					op := sv.transactions[tid].locks[x]
					if op.op == true {
						sv.store[op.key].lock.RUnlock()
					} else {
						sv.store[op.key].lock.Unlock()
					}
				}
				sv.transactions[tid].locks = nil          //release all locks
				sv.transactions[tid].state = stateVotedNo //What it votes will depend on the state that server has for that transaction.
				reply.VotedFor = false                    //vote no
				sv.mu.Unlock()
				return
			}
		} else { //set operation, regualr trylock
			if sv.store[sv.transactions[tid].ops[i].key].lock.TryLock() == true { //if lock is successful
				sv.transactions[tid].locks = append(sv.transactions[tid].locks, sv.transactions[tid].ops[i])
			} else {
				for x := 0; x < len(sv.transactions[tid].locks); x++ {
					op := sv.transactions[tid].locks[x]

					if op.op == true {
						sv.store[op.key].lock.RUnlock()
					} else {
						sv.store[op.key].lock.Unlock()
					}
				}
				sv.transactions[tid].locks = nil          //release all locks
				sv.transactions[tid].state = stateVotedNo //What it votes will depend on the state that server has for that transaction.
				reply.VotedFor = false                    //vote no
				sv.mu.Unlock()
				return
			}
		}
	}
	sv.transactions[tid].state = stateVotedYes
	reply.VotedFor = true
	sv.mu.Unlock()
}

// Abort handler
//
// This function should abort the given transaction
// Make sure to release any held locks
func (sv *Server) Abort(args *RPCArgs, reply *struct{}) {
	// Your code here
	sv.mu.Lock()
	if sv.transactions[args.Tid] == nil { //nothing to abort
		sv.mu.Unlock()
		return
	}

	for x := 0; x < len(sv.transactions[args.Tid].locks); x++ { //same as prepare
		op := sv.transactions[args.Tid].locks[x]

		if op.op == true {
			sv.store[op.key].lock.RUnlock()
		} else {
			sv.store[op.key].lock.Unlock()
		}
	}
	sv.transactions[args.Tid].locks = nil
	sv.transactions[args.Tid].state = stateAborted
	sv.mu.Unlock()
}

// Query handler
//
// This function should reply with information about all known transactions
func (sv *Server) Query(args struct{}, reply *QueryReply) {
	// Your code here
	sv.mu.Lock()
	reply.States = make(map[int]TransactionState)
	for key := range sv.transactions {
		reply.States[key] = sv.transactions[key].state //setting the map key,value == ID,state
	}
	sv.mu.Unlock()
}

// PreCommit handler
//
// This function should confirm that the server is ready to commit
// Hint: the protocol tells us to always just acknowledge preCommit,
// so there isn't too much to do here
func (sv *Server) PreCommit(args *RPCArgs, reply *struct{}) {
	// Your code here
	sv.mu.Lock()
	if sv.transactions[args.Tid] == nil {
		sv.mu.Unlock()
		return
	} else {
		sv.transactions[args.Tid].state = statePreCommitted
		sv.mu.Unlock()
	}
}

// Commit handler
//
// This function should actually apply the logged operations
// Make sure to release any held locks
func (sv *Server) Commit(args *RPCArgs, reply *CommitReply) {
	// Your code here
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if sv.transactions[args.Tid] == nil { //if no transaciton
		reply.Results = make(map[string]interface{}) //return results without committing anything, empty map
		return
	}
	if sv.transactions[args.Tid].state == stateCommitted { //if we are trying to recommit
		reply.Results = sv.transactions[args.Tid].output //just return the old output
		return
	} else {
		sv.transactions[args.Tid].output = make(map[string]interface{}) //create empty map to store output(s)
		for i := 0; i < len(sv.transactions[args.Tid].ops); i++ {       //loop through all operations in sv.transactions at index args.Tid
			if sv.transactions[args.Tid].ops[i].op == true { //if the current operation is true which we defined is a GET operation
				sv.transactions[args.Tid].output[sv.transactions[args.Tid].ops[i].key] = sv.store[sv.transactions[args.Tid].ops[i].key].value //since its a get, we get the value and set it to this key
			} else { //SET operation
				sv.store[sv.transactions[args.Tid].ops[i].key].value = sv.transactions[args.Tid].ops[i].value //replace or enter the key's valye in the store
			}
		}
	}
	for x := 0; x < len(sv.transactions[args.Tid].locks); x++ { //same as prepare
		op := sv.transactions[args.Tid].locks[x]

		if op.op == true {
			sv.store[op.key].lock.RUnlock()
		} else {
			sv.store[op.key].lock.Unlock()
		}
	}
	sv.transactions[args.Tid].locks = nil
	sv.transactions[args.Tid].state = stateCommitted
	reply.Results = sv.transactions[args.Tid].output //make sure we retirn the get stuff back
}

// Get
//
// This function should log a Get operation
func (sv *Server) Get(tid int, key string) {
	// Your code here

	sv.mu.Lock() //lock server bc we are accessing info
	defer sv.mu.Unlock()

	if sv.transactions[tid] == nil { //if the transaction doesnt exist
		sv.transactions[tid] = new(record)                         //create new transaction object &record{} creates new empty struct
		sv.transactions[tid].state = stateOperations               //servers log operations before the protocol starts
		sv.transactions[tid].output = make(map[string]interface{}) //create map to store get outputs later
	}
	newOperation := Operation{}
	newOperation.key = key
	newOperation.op = true                                                    //true cus its a get, we said get is true
	sv.transactions[tid].ops = append(sv.transactions[tid].ops, newOperation) //add the operation to the transactions list
}

// Set
//
// This function should log a Set operation
func (sv *Server) Set(tid int, key string, value interface{}) {
	// Your code here

	//practically the same procedure as get
	sv.mu.Lock() //lock server bc we are accessing info
	defer sv.mu.Unlock()

	if sv.transactions[tid] == nil {
		sv.transactions[tid] = new(record)
		sv.transactions[tid].state = stateOperations
		sv.transactions[tid].output = make(map[string]interface{})
	}

	newOperation := Operation{}
	newOperation.key = key
	newOperation.value = value
	newOperation.op = false
	sv.transactions[tid].ops = append(sv.transactions[tid].ops, newOperation)

}

// Initialize new Server
//
// keys is a slice of the keys that this server is responsible for storing
func MakeServer(keys []string) *Server {
	sv := &Server{
		// Initialize fields here
		store: make(map[string]*StoreItem), transactions: make(map[int]*record), //create empty map to store key value pairs and create map to track transactions by transaction id
	}

	for i := 0; i < len(keys); i++ { //go through all keys using the index
		sv.store[keys[i]] = &StoreItem{value: nil}
	}
	return sv
}
