package commit

import (
	"cs351/labrpc"
	"sync/atomic"
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
	// Your fields here
}

// Start the 3PC protocol for a particular transaction
// This may be called concurrently
func (co *Coordinator) FinishTransaction(tid int) {
	// Your code here

	//prepare, precommit, commit

	prepareYes := true
	precommitYes := true

	//prepare
	serversToSend := []int{}               //create list of servers we should send precommit and commit to
	for i := 0; i < len(co.servers); i++ { //i is the server
		if prepareYes == true { //while still valid
			args := &RPCArgs{Tid: tid}
			reply := &PrepareReply{}
			response := co.sendPrepare(i, args, reply)
			if response == false { //if prepare retruned false or timed out
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
	if prepareYes == false { //if prepre fail
		for i := 0; i < len(co.servers); i++ {
			args := &RPCArgs{Tid: tid}
			co.sendAbort(i, args) //we abort
			// for response == false {           //continue to send aborts until it successfully aborts
			// 	response = co.sendAbort(i, args)
			// }
		}
		co.respChan <- ResponseMsg{tid: tid, committed: false, readValues: nil}
		return
	}

	//precommit
	args := &RPCArgs{Tid: tid}
	for i := 0; i < len(serversToSend); i++ {
		if precommitYes == true {
			response := co.sendPreCommit(serversToSend[i], args)
			if response == false {
				precommitYes = false
			}
		}
	}
	if precommitYes == false { //if prepare fail
		for i := 0; i < len(co.servers); i++ {
			args := &RPCArgs{Tid: tid}
			co.sendAbort(i, args)

			// for response == false { //continue to send aborts until it successfully aborts
			// 	response = co.sendAbort(i, args)
			// }
		}
		co.respChan <- ResponseMsg{tid: tid, committed: false, readValues: nil}
		return
	}

	//commit
	outputs := make(map[string]interface{})
	for i := 0; i < len(serversToSend); i++ {
		args := &RPCArgs{Tid: tid}
		reply := &CommitReply{}
		response := co.sendCommit(serversToSend[i], args, reply)
		// if response == false { //keep trying to commit until it goes through
		// 	response = co.sendCommit(serversToSend[i], args, reply)
		// }
		for response == false && co.killed() == false{	//keep resending commit until the server responds
			reply = &CommitReply{}
			response = co.sendCommit(serversToSend[i], args, reply)
		}
		if response == true {
			for j := range reply.Results {
				outputs[j] = reply.Results[j]
			}
		}
	}
	co.respChan <- ResponseMsg{tid: tid, committed: true, readValues: outputs}

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
		// Initialize other fields here
	}

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
