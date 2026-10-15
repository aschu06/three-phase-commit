package commit

import (
	"cs351/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// Responses to the client
type ResponseMsg struct {
	tid        int
	committed  bool
	readValues map[string]interface{}
}

type Coordinator struct {
	servers  []*labrpc.ClientEnd
	respChan chan ResponseMsg
	dead     int32

	mu     sync.Mutex
	active map[int]bool //transactions this coordinator is already running, so recovery doesn't start them twice
}

// Start the 3PC protocol for a particular transaction
// This may be called concurrently
func (co *Coordinator) FinishTransaction(tid int) {
	co.mu.Lock()
	co.active[tid] = true
	co.mu.Unlock()
	//prepare, precommit, commit
	co.doPrepare(tid)
}

// Prepare phase: ask every server to vote, then move on to PreCommit or abort
func (co *Coordinator) doPrepare(tid int) {
	prepareYes := true
	serversToSend := []int{}               //create list of servers we should send precommit and commit to
	for i := 0; i < len(co.servers); i++ { //i is the server
		if prepareYes == true { //while still valid
			args := &RPCArgs{Tid: tid}
			reply := &PrepareReply{}
			response := co.sendPrepare(i, args, reply)
			if response == false { //if prepare returned false or timed out
				prepareYes = false
			} else {
				if reply.VotedFor == false {
					prepareYes = false
				}
				if reply.Relevant == true {
					serversToSend = append(serversToSend, i)
				}
			}
		}
	}
	if co.killed() { //coordinator crashed mid-phase, recovery on the new coordinator takes over
		return
	}
	if prepareYes == false { //if prepare fail
		co.doAbort(tid)
		return
	}
	co.doPreCommit(tid, serversToSend)
}

// PreCommit phase: send PreCommit to relevant servers, then move on to Commit or abort
func (co *Coordinator) doPreCommit(tid int, servers []int) {
	precommitYes := true
	args := &RPCArgs{Tid: tid}
	for i := 0; i < len(servers); i++ {
		if precommitYes == true {
			response := co.sendPreCommit(servers[i], args)
			if response == false {
				precommitYes = false
			}
		}
	}
	if co.killed() {
		return
	}
	if precommitYes == false { //if precommit fail
		co.doAbort(tid)
		return
	}
	co.doCommit(tid, servers)
}

// Commit phase: resend Commit until each relevant server responds, then report success
func (co *Coordinator) doCommit(tid int, servers []int) {
	outputs := make(map[string]interface{})
	for i := 0; i < len(servers); i++ {
		args := &RPCArgs{Tid: tid}
		reply := &CommitReply{}
		response := co.sendCommit(servers[i], args, reply)
		for response == false && co.killed() == false { //keep resending commit until the server responds
			time.Sleep(10 * time.Millisecond)
			reply = &CommitReply{}
			response = co.sendCommit(servers[i], args, reply)
		}
		if response == true {
			for j := range reply.Results {
				outputs[j] = reply.Results[j]
			}
		}
	}
	if co.killed() {
		return
	}
	co.respChan <- ResponseMsg{tid: tid, committed: true, readValues: outputs}
}

// Abort: report the abort to the client, then resend Abort to every server until it responds
func (co *Coordinator) doAbort(tid int) {
	if co.killed() {
		return
	}
	co.respChan <- ResponseMsg{tid: tid, committed: false, readValues: nil} //no need to wait for disconnected servers, the outcome is already decided
	args := &RPCArgs{Tid: tid}
	for i := 0; i < len(co.servers); i++ {
		response := co.sendAbort(i, args)
		for response == false && co.killed() == false { //keep resending abort until the server responds
			time.Sleep(10 * time.Millisecond)
			response = co.sendAbort(i, args)
		}
	}
}

// Recovery: query every server for the state of each transaction it knows about,
// then resume each unfinished transaction from the right phase
func (co *Coordinator) recover() {
	//states[tid][server] = that server's state for the transaction
	states := make(map[int]map[int]TransactionState)
	for i := 0; i < len(co.servers); i++ {
		reply := &QueryReply{}
		response := co.sendQuery(i, reply)
		for response == false && co.killed() == false { //keep resending query until the server responds
			time.Sleep(10 * time.Millisecond)
			reply = &QueryReply{}
			response = co.sendQuery(i, reply)
		}
		if co.killed() {
			return
		}
		for tid, state := range reply.States {
			if states[tid] == nil {
				states[tid] = make(map[int]TransactionState)
			}
			states[tid][i] = state
		}
	}

	//only resume transactions after every server has been queried
	for tid, serverStates := range states {
		servers := []int{} //servers that know about a transaction are the relevant ones
		anyAbort, allAborted, anyCommitted, allCommitted, anyPreCommitted, anyVotedYes := false, true, false, true, false, false
		for server, state := range serverStates {
			servers = append(servers, server)
			if state == stateVotedNo || state == stateAborted {
				anyAbort = true
			}
			if state != stateAborted {
				allAborted = false
			}
			if state == stateCommitted {
				anyCommitted = true
			} else {
				allCommitted = false
			}
			if state == statePreCommitted {
				anyPreCommitted = true
			}
			if state == stateVotedYes {
				anyVotedYes = true
			}
		}

		co.mu.Lock()
		alreadyRunning := co.active[tid] //started on this coordinator by FinishTransaction while we were querying
		co.active[tid] = true
		co.mu.Unlock()
		if alreadyRunning {
			continue
		}

		//apply the recovery rules in order
		if anyAbort {
			if !allAborted { //if every server already aborted, the transaction is finished
				go co.doAbort(tid)
			}
		} else if anyCommitted && !allCommitted {
			go co.doCommit(tid, servers)
		} else if anyPreCommitted {
			go co.doPreCommit(tid, servers)
		} else if anyVotedYes {
			go co.doPrepare(tid)
		}
	}
}

// Initialize new Coordinator
//
// This will be called at the beginning of a test to create a new Coordinator
// It will also be called when the Coordinator restarts, so you'll need to trigger recovery here
// respChan is how you'll send messages to the client to notify it of committed or aborted transactions
func MakeCoordinator(servers []*labrpc.ClientEnd, respChan chan ResponseMsg) *Coordinator {
	co := &Coordinator{
		servers:  servers,
		respChan: respChan,

		active: make(map[int]bool),
	}

	//recover in the background, since the tester calls this while holding its own lock
	go co.recover()

	return co
}

// Like in Raft, each send method returns true if the request succeeded and false if it timed out
// They are guaranteed to return *unless* the handler function on the server side does not return

func (co *Coordinator) sendPrepare(server int, args *RPCArgs, reply *PrepareReply) bool {
	return co.servers[server].Call("Server.Prepare", args, reply)
}

func (co *Coordinator) sendAbort(server int, args *RPCArgs) bool {
	reply := struct{}{}
	return co.servers[server].Call("Server.Abort", args, &reply)
}

func (co *Coordinator) sendQuery(server int, reply *QueryReply) bool {
	return co.servers[server].Call("Server.Query", struct{}{}, reply)
}

func (co *Coordinator) sendPreCommit(server int, args *RPCArgs) bool {
	reply := struct{}{}
	return co.servers[server].Call("Server.PreCommit", args, &reply)
}

func (co *Coordinator) sendCommit(server int, args *RPCArgs, reply *CommitReply) bool {
	return co.servers[server].Call("Server.Commit", args, reply)
}

// The tester doesn't halt goroutines created by the Coordinator after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (co *Coordinator) Kill() {
	atomic.StoreInt32(&co.dead, 1)
	// Your code here, if desired.
}

func (co *Coordinator) killed() bool {
	z := atomic.LoadInt32(&co.dead)
	return z == 1
}