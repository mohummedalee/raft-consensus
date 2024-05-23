package main

import (
    "fmt"
)

type RequestVoteArgs struct {
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

type RequestVoteReply struct {
    Term int
    VoteGranted bool
}

/* Handle an election whenever the term times out without a heartbeat */
func (rf *Raft) handleElectionTimeouts() {
    for {
        select {
            case <-rf.electionTicker.C:
                rf.mu.Lock()
                if rf.State == Leader {
                    // leader doesn't need to wait for heartbeats
                    rf.electionTicker = randomTicker(rf.ElectionTime, rf.ElectionTime*2)
                    rf.mu.Unlock()
                    continue
                }
                fmt.Println("TERM: ", rf.CurrentTerm,"TIMEOUT")
                rf.State = Candidate
                rf.VotedFor = -1
                rf.resetElectionState()
                rf.CurrentTerm++
                rf.electionTicker = randomTicker(rf.ElectionTime, rf.ElectionTime*2)

                args := RequestVoteArgs {
                    Term: rf.CurrentTerm,
                    CandidateId: rf.Me,
                    LastLogIndex: len(rf.Log),
                    LastLogTerm: rf.getLastLogTerm(),
                }
                for _, v := range rf.Peers {
                    if v != myName() {
                        // ask for vote
                        var reply RequestVoteReply
                        go rf.callRequestVote(args, &reply, v, port)

                    }
                    if v == myName() {
                        // vote for self right away
                        rf.VotedFor = rf.Me
                        rf.ElectionState[v] = true
                    }

                }
                rf.mu.Unlock()
            case <-rf.quit:
                // got quit signal, so quit this loop
                return
        }
    }
}


// Request Vote RPC
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
    rf.mu.Lock()

    // RequestVote has an outdated term, so don't grant the vote
    if args.Term < rf.CurrentTerm {
        reply.VoteGranted = false
        reply.Term = rf.CurrentTerm
        rf.mu.Unlock()
        return nil
    }

    // candidate has already voted for self; should not re-vote this term
    if rf.CurrentTerm == args.Term && rf.State == Candidate {

        //log.Println("TERM", rf.CurrentTerm,": not granting vote request because terms are equal")
        reply.VoteGranted = false
        reply.Term = rf.CurrentTerm
        rf.mu.Unlock()
        //log.Println("released lock: RequestVote")
        return nil
    }

    // grant vote
    if (args.Term > rf.CurrentTerm) ||
    ((rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && rf.candidateLogsMatch(args)){
        reply.Term = rf.CurrentTerm
        // EXPERIMENTAL
        rf.CurrentTerm = args.Term
        rf.stepDown(args.Term)
        reply.VoteGranted = true
        rf.State = Follower
        rf.VotedFor = args.CandidateId
        rf.resetElectionTimer()
        rf.mu.Unlock()
        return nil
    }

    rf.mu.Unlock()
    return nil
}

/* This instance has been elected. Set state to leader and reset NextIndex and MatchIndex for all followers.
Start sending heartbeats. */
func (rf *Raft) becomeLeader() {
    fmt.Println("TERM: ", rf.CurrentTerm,"LEADER")
    rf.resetElectionState()
    rf.State = Leader
    rf.resetElectionTimer()
    rf.VotedFor = -1

    for k := range rf.Peers {
        // initialize NextIndex to last Log index + 1
        // last Log index is equal to length of Log because Log is 1-indexed
        rf.NextIndex[k] = len(rf.Log)+1
        rf.MatchIndex[k] = 0
    }

    go rf.sendHeartbeats()
}
