package raft

import (
	"log"
	"math"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Println(a...)
	}
	return
}

func RandomInt(left, right int) int {
	return rand.Intn(right-left+1) + left
}

type TimeoutType int

const (
	AppendEntriesTimeout TimeoutType = iota
	ElectionTimeout
)

func getTimeoutByType(timeoutType TimeoutType) <-chan time.Time {
	switch timeoutType {
	case ElectionTimeout:
		return time.Tick(time.Duration(RandomInt(500, 1000)) * time.Millisecond)
	case AppendEntriesTimeout:
		return time.Tick(time.Duration(RandomInt(500, 1000)) * time.Millisecond)
	}
	return nil
}

func minInt(arr ...int) int {
	min := math.MaxInt64
	for _, v := range arr {
		if v < min {
			min = v
		}
	}
	return min
}
