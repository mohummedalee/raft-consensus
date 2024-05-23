package main

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// listen for incoming RPCs
func serve(port string) {
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("net.Listen: ", e)
	}

	err := http.Serve(listener, nil)
	if err != nil {
		log.Fatal("http.Serve: ", err)
	}
}

// contact peer at the given port number
func httpDial(host string, port string) (*rpc.Client, error) {
	// contact a peer
	hostport := host+":"+port
	client, err := rpc.DialHTTP("tcp", hostport)

	attempts := 0
	for err != nil {
		if attempts > 1 {
			return nil, errors.New("couldn't contact client")
		}
		// failed to contact peer, try again
		time.Sleep(1 * time.Second)
		client, err = rpc.DialHTTP("tcp", hostport)
		attempts++
	}

	return client, nil
}

// call the RequestVote RPC for given peer at given port number
func (rf *Raft) callRequestVote(args RequestVoteArgs, reply *RequestVoteReply, host string, port string) {
	client, err := httpDial(host, port)
	if err != nil {
		// couldn't contact peer
		return
	}

	err = client.Call("Raft.RequestVote", args, reply)
	if err != nil {
		return
	}

	// if you got a response with a greater term ID
	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		rf.stepDown(reply.Term)
		rf.mu.Unlock()
		return
	}

	// check for reply
	if reply.VoteGranted {
		rf.ElectionState[host] = true
	}

	if rf.haveElectionQuorum() {
		// only become leader if we're not already the leader
		if rf.State != Leader {
			rf.becomeLeader()
		}
	}
	rf.mu.Unlock()

	err = client.Close()
	if err != nil {
		return
	}
}

// call AppendEntries RPC with emtpy log entries (heartbeat RPC)
func (rf *Raft) callAppendEntriesHeartbeat(args AppendEntriesArgs, reply *AppendEntriesReply, host string, port string) {
	client, err := httpDial(host, port)
	if err != nil {
		// couldn't contact peer
		return
	}

	err = client.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		return
	}

	if !reply.Success {
		//log.Println("heartbeat not successful")
	}

	err = client.Close()
	if err != nil {
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		// convert to follower
		rf.stepDown(reply.Term)
	}
	rf.mu.Unlock()
}

// call AppendEntries RPC with log entries to replicate on peers
func (rf *Raft) callAppendEntriesLog(args AppendEntriesArgs, reply *AppendEntriesReply, host string, port string) {
	client, err := httpDial(host, port)
	if err != nil {
		// couldn't contact peer
		return
	}

	err = client.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		return
	}

	err = client.Close()
	if err != nil {
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		// convert to follower
		rf.stepDown(reply.Term)
	}
	rf.mu.Unlock()
}
