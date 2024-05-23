package main

import (
	"fmt"
	"log"
	"os"
	"bufio"
	"strconv"
)

// get hostname of this instance
func myName() string {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal("os.Hostname():", err)
	}
	return name
}

// load hosts from file and return map and ID
func loadFiles(fName string) (map[int]string, int, string) {
	hostMap := make(map[int]string)

	f, err := os.Open(fName)
	if err != nil {
		log.Fatal("os.Open", err)
	}
	scanner := bufio.NewScanner(f)
	name := myName()
	id := -1
	var i int = 1
	for scanner.Scan() {
		hostname := scanner.Text()
		hostMap[i] = hostname
		if name == hostname {
			id = i
		}
		i++
	}
	f.Close()

	return hostMap, id, name
}

func getIntegerFromString(str string) (int, bool) {
	key, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println("Cannot convert ", str, " to integer. Key must be an integer.")
		return 0, false
	}
	return key, true
}

