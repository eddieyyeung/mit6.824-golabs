package raft

type Log struct {
	Command interface{}
	Term    int
	Index   int
}
