package main

import (
	"encoding/gob"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// enum for replica State
type ReplicaState int
const (
	Follower	ReplicaState = 1
	Candidate	ReplicaState = 2
	Leader		ReplicaState = 3
)

// the state of each Raft instance
type Raft struct {
	Peers          map[int]string         // host id --> port addresses
	Me             int                    // id of this node
	mu             sync.Mutex             // lock for preventing concurrent updates
	CurrentTerm    int                    // current term index
	VotedFor       int                    // id of node I voted for
	Log            []LogEntry             // log entries
	State          ReplicaState           // the state of the node (Leader, Follower, or Candidate)
	ElectionTime   int                    // constant for election timeouts
	electionTicker *time.Ticker           // ticker for election timeouts
	HeartbeatTime  int                    // constant for heartbeat timeouts
	ElectionState  map[string]bool        // store votes received
	quit           chan int               // channel for quitting election loop
	CommitIndex    int                    // index of highest log entry known to be committed
	LastApplied    int                    // index of highest log entry applied to state machine
	LeaderID       int                    // store leader ID so it can be forwarded to client
	StateMachine   map[int]string         // replicated state machine
	NextIndex      map[int]int            // server id --> the index of the next log entry to send to that server
	MatchIndex     map[int]int            // server id --> index of highest log entry known to be replicated on that server
}

// load state from file if one is found, else return false
func (rf *Raft) loadPersistentState(me int) bool {
	f, err := os.Open("savedState" + strconv.Itoa(me)+ ".gob")
	if err != nil {
		//log.Println("no State found")
		return false
	}
	decoder := gob.NewDecoder(f)
	err = decoder.Decode(rf)
	if err != nil {
		log.Println("decoder:", err)
	}

	f.Close()
	return true
}

// save the Raft struct to a file as an encoded gob
func (rf *Raft) savePersistentState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	f, err := os.Create("savedState" + strconv.Itoa(rf.Me) + ".gob")
	if err != nil {
		log.Println("could not save State")
		return
	}
	encoder := gob.NewEncoder(f)
	err = encoder.Encode(*rf)
	if err != nil {
		log.Fatal("encoder:", err)
	}
}

/* Initializes a new Raft instance, either reading from the saved state or starting
an entirely new instance. */
func MakeInstance(peers map[int]string, me int) *Raft {
	rf := new(Raft)
	wasRunning := rf.loadPersistentState(me)
	if !wasRunning {
		rf.Peers = peers
		rf.Me = me
		rf.State = Follower
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		rf.Log = []LogEntry{}
		rf.CommitIndex = 0
		rf.LastApplied = 0
		rf.LeaderID = 0
		rf.StateMachine = make(map[int]string)
		rf.NextIndex = make(map[int]int)
		rf.MatchIndex = make(map[int]int)

		for k := range rf.Peers {
			// initialize NextIndex to last Log index + 1
			// last Log index is = length of Log because Log is 1-indexed
			rf.NextIndex[k] = len(rf.Log)+1
			rf.MatchIndex[k] = 0
		}
	} else {
	}

	// initialize more stuff that wasn't part of saved state
	rf.quit = make(chan int)
	// random election timer b/w 1000-2000 ms
	rf.ElectionTime = 1000
	rf.electionTicker = randomTicker(rf.ElectionTime, rf.ElectionTime*2)
	rf.resetElectionState()
	rf.HeartbeatTime = 50

	// register for RPCs and start listening
	err := rpc.Register(rf)
	if err != nil {
		log.Fatal("rpc.Register: ", err)
	}
	go serve(port)
	go rf.handleElectionTimeouts()

	return rf
}

/* return true if this candidate has received a majority of votes */
func (rf *Raft) haveElectionQuorum() bool {
	votes := 0.0
	for _, v := range rf.ElectionState {
		if v {
			votes++
		}
	}
	
	return votes >= float64(len(rf.ElectionState))/2.0
}

/* reset election state when a candidate is no longer a candidate */
func (rf *Raft) resetElectionState() {
	rf.ElectionState = make(map[string]bool)
	for _, v := range rf.Peers {
		rf.ElectionState[v] = false
	}
}

// convert raft instance to a follower (leader de-election)
func (rf *Raft) stepDown(term int) {
	if rf.State != Follower {
		rf.State = Follower
		rf.CurrentTerm = term
	}
}
