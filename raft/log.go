package main

import "log"

type LogEntry struct {
	Key int
	Value string
	Term int
	Index int
}

// true if the candidate logs match the last log term and index received in the RequestVote args
func (rf *Raft) candidateLogsMatch(args RequestVoteArgs) bool {
	return rf.getLastLogTerm() == args.LastLogTerm && rf.getLastLogIndex() == args.LastLogIndex
}

// return term of last entry in the log
func (rf *Raft) getLastLogTerm() int {
	if len(rf.Log) > 0 {
		return rf.Log[len(rf.Log)-1].Term
	} else {
		return -1
	}
}

// return index of last entry in the log (logs are 1-indexed)
func (rf *Raft) getLastLogIndex() int {
	return len(rf.Log)
}

// return true if the previous term of the AppendEntries logs matches the term at prevLogIndex-1. */
func (rf *Raft) prevTermMatches(args AppendEntriesArgs) bool {

	if len(rf.Log) == 0 {
		// empty Log
		if len(args.Entries) == 0 {
			return true
		} else if args.Entries[0].Index == 1 {
			return true
		} else {
			return false
		}
	} else if args.PrevLogTerm == 0 && args.PrevLogIndex == 0 {
		// heartbeat
		return true
	} else {
		rf.mu.Lock()
		var match bool
		if args.PrevLogIndex == 0 {
			// i'm being sent the full list
			match = true
		} else if args.PrevLogIndex - 1 >= len(rf.Log) {
			// index out of range of log
			match = false
		} else {
			// here, we do the actual check that the previous log terms match
			match = rf.Log[args.PrevLogIndex-1].Term == args.PrevLogTerm
		}
		rf.mu.Unlock()
		return match
	}
}

/* Called by Leader.
Naively adds a Log entry without overwriting or changing any entries.
Called from addLogEntry, where we've already acquired lock. */
func (rf *Raft) append(key int, value string) int {
	logIndex := len(rf.Log) + 1
	rf.Log = append(rf.Log, LogEntry{key, value, rf.CurrentTerm, logIndex})
	return logIndex
}

/* Called by a follower.
Overwrite Log with new entry, making sure to clear any subsequent entries.*/
func (rf *Raft) overwrite(key int, value string, index int) {
	logLength := len(rf.Log)
	if logLength < index {
		if index - logLength > 1 {
			log.Fatal("Tried to append Log entry at index : ", index, " but logLength is : ", logLength)
		}
		// naive append
		rf.append(key, value)
	} else {
		// clear subsequent entries
		rf.Log = rf.Log[:index-1]
		// add new entry
		rf.append(key, value)
	}
}


