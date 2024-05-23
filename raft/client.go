package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Client instance; interacts with Raft cluster
type RaftClient struct {
	hosts []string
	lastLeader string
	port string
}

type PushArgs struct {
	Key int
	Value string
}

type PushReply struct {
	Success bool
	LeaderAddress string

}

type GetArgs struct {
	Key int
}

type GetReply struct {
	Success bool
	LeaderAddress string
	Value string
}

/* Client program */
func makeRaftClient(hostMap map[int]string, port string) {
	cl := RaftClient{}
	cl.port = port

	cl.hosts = []string{}
	for _, hostname := range hostMap {
		cl.hosts = append(cl.hosts, hostname)
	}

	cl.lastLeader = cl.randomLeader()

	fmt.Println("Commands : 'put key value' or 'get key' or 'Q' to Quit.")

	for {
		fmt.Print("ENTER A COMMAND : ")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		//fmt.Println("Processing...")
		quit := cl.handleClientString(text)
		if quit {
			return
		}
	}
}

/* Handle user's command line input.
Commands :   get key             - get value at given key in store
             put key value       - put key-value pair in store
             Q                   - quit
 */
func (cl *RaftClient) handleClientString(str string) bool {
	if str == "Q\n" {
		log.Println("CLIENT QUIT.")
		return true
	}

	words := strings.Fields(str)
	if len(words) == 0 {
		fmt.Println("No command received.")
	} else if words[0] == "put" {
		if len(words) != 3 {
			fmt.Println("Put command not valid. Syntax is 'put key value', e.g. 'put 3 foo'.")
		} else {
			key, success := getIntegerFromString(words[1])
			if !success {
				return false
			}
			value := words[2]
			//fmt.Println("Key : ", key, " Value : ", value)
			cl.push(key, value)
		}
	} else if words[0] == "get" {
		if len(words) != 2 {
			fmt.Println("Get command not valid. Syntax is 'get key', e.g. 'get 3'.")
		} else {
			key, success := getIntegerFromString(words[1])
			if !success {
				return false
			}
			//fmt.Println("KEY : ", key)
			cl.get(key)
		}
	} else {
		fmt.Println("Bad input. Operation not recognized.")
	}

	return false
}

/* Select a random leader. Used by client if last leader is unknown or unreachable.  */
func (cl *RaftClient) randomLeader() string {
	random := rand.Int() % len(cl.hosts)
	return cl.hosts[random]
}

/* Client selects random node or last known leader and sends a Put request. */
func (cl *RaftClient) push(key int, value string) {
	leaderAddress := cl.lastLeader

	args := PushArgs{key, value}
	reply := PushReply{}
	callPush(args, &reply, leaderAddress, cl.port)
	attempts := 0
	for !reply.Success {
		if reply.LeaderAddress != "" {
			// Raft instance sent us a leader, so we should use that leader
			cl.lastLeader = reply.LeaderAddress
			leaderAddress = cl.lastLeader
		} else {
			// choose a new random leader
			cl.lastLeader = cl.randomLeader()
			leaderAddress = cl.lastLeader
		}
		callPush(args, &reply, leaderAddress, cl.port)
		attempts++
		if attempts > 5 {
			fmt.Println("RESPONSE: No response from cluster. Check if key-value pair ", key, " ", value, " was saved.")
			return
		}
	}
	fmt.Println("RESPONSE: Key: ", key, " Value: ", value, " saved.")
}

/* Client sends Push request to given Raft node. */
func callPush(args PushArgs, reply *PushReply, leaderAddress string, port string) {
	//log.Println("CLIENT: contacting ", leaderAddress, " to push value.")
	client, err := httpDial(leaderAddress, port)
	if err != nil {
		// couldn't contact peer
		return
	}

	err = client.Call("Raft.Push", args, reply)
	if err != nil {
		log.Println("client.Call: ", err)
		return
	}

	err = client.Close()
	if err != nil {
		// FIXME: this error doesn't actually matter
		log.Println("client.Close()")
	}
}

/* Client selects random node or previous known leader and sends a get request. */
func (cl *RaftClient) get(key int) {
	leaderAddress := cl.lastLeader

	args := GetArgs{key}
	reply := GetReply{}
	callGet(args, &reply, leaderAddress, cl.port)
	attempts := 0
	for !reply.Success {
		if reply.LeaderAddress != "" {
			// Raft instance sent us a leader, so we should use that leader
			cl.lastLeader = reply.LeaderAddress
			leaderAddress = cl.lastLeader
		} else {
			// choose a new random leader
			cl.lastLeader = cl.randomLeader()
			leaderAddress = cl.lastLeader
		}
		callGet(args, &reply, leaderAddress, cl.port)
		attempts++
		if attempts > 5 {
			fmt.Println("Cluster non-responsive. Try again.")
			//fmt.Println("RESPONSE: Get failed for key : ", key)
			return
		}
	}
	if reply.Value == "" {
		fmt.Println("RESPONSE: Key ", key, "doesn't exist in DB.")
	} else {
		fmt.Println("RESPONSE: Key: ", key, " Value: ", reply.Value)
	}
}

/* Client sends get request to a Raft node. */
func callGet(args GetArgs, reply *GetReply, leaderAddress string, port string) {
	//log.Println("CLIENT: contacting ", leaderAddress, ".")
	client, err := httpDial(leaderAddress, port)
	if err != nil {
		// couldn't contact peer
		return
	}

	err = client.Call("Raft.Get", args, reply)
	if err != nil {
		log.Println("client.Call: ", err)
		return
	}

	err = client.Close()
	if err != nil {
		// FIXME: this error doesn't actually matter
		log.Println("client.Close()")
	}
}

/* Push RPC to put a key-value pair in the key-value store. */
func (rf *Raft) Push(args PushArgs, reply *PushReply) error {

	if rf.State != Leader {
		reply.Success = false
		reply.LeaderAddress = rf.Peers[rf.LeaderID]
	} else {
		success := rf.addLogEntry(args.Key, args.Value)
		reply.Success = success
		reply.LeaderAddress = rf.Peers[rf.Me]
	}
	return nil
}

/* Get RPC to get a value from the key-value store. */
func (rf *Raft) Get(args GetArgs, reply *GetReply) error {
	if rf.State != Leader {
		reply.Success = false
		reply.LeaderAddress = rf.Peers[rf.LeaderID]
	} else {
		rf.mu.Lock()
		fmt.Println("GET ", args.Key)
		if value, okay := rf.StateMachine[args.Key]; okay {
			reply.Success = true
			reply.Value = value
		} else {
			reply.Success = true
			reply.Value = ""
		}
		rf.mu.Unlock()
	}
	return nil
}

/* Add log entry to my own log.

Wait until it's replicated on a majority of nodes before responding to client.
(Prioritizes data integrity over availability for client). */
func (rf *Raft) addLogEntry(key int, value string) bool {
	rf.mu.Lock()
	fmt.Println("PUT ", key, " ", value)
	logIndex := rf.append(key, value)
	rf.mu.Unlock()

	for peer, peerName := range rf.Peers {
		if peerName != myName() {
			go rf.replicateLog(peer)
		}
	}

	for !rf.hasLogQuorum(logIndex) {
		// wait until we have a quorum
		if rf.State != Leader {
			// give up
			return false
		}
		time.Sleep(time.Duration(10)*time.Millisecond)
	}

	// we have a quorum
	rf.leaderUpdateCommitIndex(logIndex)
	rf.applyStateToMachine()
	return true
}

/* Build AppendEntriesArgs for a single follower. */
func (rf *Raft) buildAppendEntryArgs(follower int) (AppendEntriesArgs, bool){
	lastLogIndex := len(rf.Log)
	if lastLogIndex >= rf.NextIndex[follower] {
		// construct AppendEntriesArgs

		// NextIndex contains a 1-based logIndex
		logIndexToSend := rf.NextIndex[follower]

		return AppendEntriesArgs{
			rf.CurrentTerm,
			rf.Me,
			logIndexToSend-1,
			rf.Log[logIndexToSend-1].Term,
			rf.Log[logIndexToSend-1:],
			rf.CommitIndex,
		}, true

	} else {
		//log.Println("for follower", follower, "shouldupdate = false")
		return AppendEntriesArgs{}, false
	}

}

/* Send log replication message to a single follower */
func (rf *Raft) replicateLog(follower int) {
	rf.mu.Lock()
	args, shouldUpdate := rf.buildAppendEntryArgs(follower)
	if !shouldUpdate {
		rf.mu.Unlock()
		return
	}
	var reply AppendEntriesReply
	rf.mu.Unlock()

	rf.callAppendEntriesLog(args, &reply, rf.Peers[follower], port)

	for !reply.Success {
		rf.mu.Lock()
		// NextIndex is 1 indexed - shouldn't be less than 1
		if rf.NextIndex[follower] != 1 {
			rf.NextIndex[follower]--
		}
		args, shouldUpdate = rf.buildAppendEntryArgs(follower)
		if !shouldUpdate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.callAppendEntriesLog(args, &reply, rf.Peers[follower], port)
	}

	// successful reply
	rf.mu.Lock()
	rf.NextIndex[follower] = args.Entries[len(args.Entries)-1].Index + 1
	rf.MatchIndex[follower] = args.Entries[len(args.Entries)-1].Index
	rf.mu.Unlock()
}

/* Check if this log entry has been replicated on a majority of nodes. */
func (rf *Raft) hasLogQuorum(logIndex int) bool {
	replicas := 0.0
	for _, e := range rf.MatchIndex {
		if e >= logIndex {
			replicas++
		}
	}
	majority := float64(len(rf.ElectionState))/2.0
	// majority-1 because we assume to have replicated on ourselves
	if replicas >= majority-1 {
		return true
	} else {
		return false
	}
}

/* Update commit index for leader to the given committed logIndex .*/
func (rf *Raft) leaderUpdateCommitIndex(logIndex int) {
	rf.mu.Lock()
	if (logIndex > rf.CommitIndex) &&
			(rf.hasLogQuorum(logIndex)) &&
			(rf.Log[logIndex-1].Term == rf.CurrentTerm) {
		rf.CommitIndex = logIndex
	}
	rf.mu.Unlock()
}

/* Apply any committed but un-applied operations to state machine.*/
func (rf *Raft) applyStateToMachine() {
	rf.mu.Lock()
	for rf.LastApplied < rf.CommitIndex {
		rf.LastApplied++
		key := rf.Log[rf.LastApplied-1].Key
		value := rf.Log[rf.LastApplied-1].Value
		rf.StateMachine[key] = value
	}
	rf.mu.Unlock()
	// save state to a file each time we apply it
	rf.savePersistentState()
}

/*  Update commit index of this follower.
Called from AppendEntries, where we acquired the lock.*/
func (rf *Raft) followerUpdateCommitIndex(args AppendEntriesArgs) {
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = int(math.Floor(math.Min(float64(args.LeaderCommit), float64(len(rf.Log)))))
	}
}

