package raft

import "sync"

// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%v][AppendEntries]", rf.me)
	// 1. Reply false if term < currentTerm (§5.1)
	currentTerm := rf.getCurrentTerm()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		DPrintf("[%v][AppendEntries] a", rf.me)
		return
	}

	// TODO: 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

	// TODO: 3. If an existing entry conflicts with a new one (same index but different terms),

	// TODO: 4. Append any new entries not already in the log

	// TODO: 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	reply.Term = args.Term
	reply.Success = true
	rf.roleWant <- RoleWant{
		Term: args.Term,
		Role: Follower,
	}
	DPrintf("[%v][AppendEntries] b", rf.me)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAllAppendEntries(ch chan RoleWant) {
	DPrintf("[%v][sendAllAppendEntries]", rf.me)
	var wg sync.WaitGroup
	currentTerm := rf.getCurrentTerm()
	go func() {
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			wg.Add(1)
			go func(server int) {
				args := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      nil,
					LeaderCommit: 0,
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if ok && reply.Term > args.Term {
					ch <- RoleWant{
						Term: reply.Term,
						Role: Follower,
					}
				}
				wg.Done()
			}(index)
		}
		wg.Wait()
		ch <- RoleWant{
			Term: currentTerm,
			Role: Leader,
		}
	}()
}
