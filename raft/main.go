package main

import (
	"flag"
	"fmt"
	"log"
)

const port string = "20260"
const hostfile string = "hostfile.txt"


func main() {
	hostMap, myId, _ := loadFiles(hostfile)
	seedRandomGenerator()
	
	// parse command line args
	clientPtr := flag.Bool("client", false, "a bool")
	flag.Parse()

	if *clientPtr {
		fmt.Println("RAFT CLIENT")
		makeRaftClient(hostMap, port)
		return
	} else {
		if myId == -1 {
			log.Fatal("hostname of this instance doesn't match hostname in hostfile.txt. Use -client flag for client application.")
			return
		}
		fmt.Println("RAFT SERVER: ", myId)
		// Raft instance
		MakeInstance(hostMap, myId)

		endRaft := make(chan bool)
		for <-endRaft {
			log.Println("exiting main")
			return
		}
	}
}
