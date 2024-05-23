package main

import (
	"math/rand"
	"time"
)

func seedRandomGenerator() {
	rand.Seed(time.Now().Unix())
}

func randomTicker(min_milliseconds int, max_milliseconds int) *time.Ticker {
	t := rand.Intn(max_milliseconds - min_milliseconds) + min_milliseconds
	return time.NewTicker(time.Duration(t)*time.Millisecond)
}

func (rf *Raft) resetElectionTimer() {
	rf.quit <- 0
	rf.electionTicker = randomTicker(rf.ElectionTime, rf.ElectionTime*2)
	go rf.handleElectionTimeouts()
}
