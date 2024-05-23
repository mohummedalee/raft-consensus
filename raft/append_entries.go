package main

import (
	"fmt"
	"log"
	"time"
)

// reply for the AppendEntries RPC
type AppendEntriesReply struct {
	Term int
	Success bool
}

// arguments for the AppendEntries RPC
type AppendEntriesArgs struct {
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

// AppendEntries RPC
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {

	if args.Term < rf.CurrentTerm || !rf.prevTermMatches(args) {
		// term is old, so reject/ignore this AppendEntries message
		reply.Success = false
		return nil
	} else {
		rf.mu.Lock()
		rf.LeaderID = args.LeaderID
		// reset any State that might have been changed during election -- we have a new leader
		rf.VotedFor = -1
		rf.resetElectionTimer()
		if rf.CurrentTerm == args.Term && rf.State == Leader {
			log.Fatal("Leader got a heartbeat with the same term as its own. FATAL.")
		}
		rf.stepDown(args.Term)
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()

		if len(args.Entries) == 0 { // heartbeat
			fmt.Println("TERM: ", rf.CurrentTerm,"❤️")
			rf.CurrentTerm = args.Term
			reply.Success = true
			rf.mu.Lock()
			rf.followerUpdateCommitIndex(args)
			rf.mu.Unlock()
			rf.applyStateToMachine()
			return nil
		} else {
			// not heartbeat
			rf.mu.Lock()
			for _, logEntry := range args.Entries {
				rf.overwrite(logEntry.Key, logEntry.Value, logEntry.Index)
				reply.Success = true
			}
			rf.followerUpdateCommitIndex(args)
			rf.mu.Unlock()
			rf.applyStateToMachine()
		}
	}

	return nil
}

// Loop for sending heartbeats as long as this instance remains the leader
func (rf *Raft) sendHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.State == Leader {
			// send heartbeats to all Peers
			for _, v := range rf.Peers {
				if v != myName() {
					var prevLogIndex int = 0
					var prevLogTerm int = 0

					var reply AppendEntriesReply
					args := AppendEntriesArgs{
						rf.CurrentTerm,
						rf.Me,
						prevLogIndex,
						prevLogTerm,
						[]LogEntry{},
						rf.CommitIndex,
					}
					go rf.callAppendEntriesHeartbeat(args, &reply, v, port)
				}
			}
		} else {
			// leader was de-elected, quit sending heartbeats
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(rf.HeartbeatTime)* time.Millisecond)
	}
}
