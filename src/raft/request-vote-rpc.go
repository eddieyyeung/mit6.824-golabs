package raft

import (
	"sync"
	"sync/atomic"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// Invoked by candidates to gather votes (§5.2).
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last logs entry (§5.4)
	LastLogTerm  int // term of candidate’s last logs entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%v][RequestVote] [1.] [reply:%#v]", rf.me, reply)
		return
	}

	// TODO: 2. If votedFor is null or candidateId, and candidate’s logs is at least as up-to-date as receiver’s logs, grant vote
	if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
		(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1) {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = args.Term
		go func() {
			rf.roleWantCh <- RoleWant{
				Term: args.Term,
				Role: Follower,
			}
		}()
		DPrintf("[%v][RequestVote] [2.] [reply:%#v]", rf.me, reply)
		return
	}

	// TODO: term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.VoteGranted = false
		reply.Term = args.Term
		if rf.role != Follower {
			rf.votedFor = -1
			go func() {
				rf.roleWantCh <- RoleWant{
					Term: args.Term,
					Role: Follower,
				}
			}()
		}
		DPrintf("[%v][RequestVote] [3.] [reply:%#v]", rf.me, reply)
		return
	}
	reply.VoteGranted = false
	reply.Term = args.Term
	DPrintf("[%v][RequestVote] [4.] [reply:%#v]", rf.me, reply)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAllRequestVote(ch chan RoleWant) {
	rf.mu.Lock()
	var wg sync.WaitGroup
	DPrintf("[%v][sendAllRequestVote] [Term:%v]", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	rf.mu.Unlock()
	var voteCount int32 = 1
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			DPrintf("[%v][sendAllRequestVote] server:%v args:%#v reply:%#v", rf.me, server, args, reply)

			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
			if ok && reply.Term > currentTerm {
				go func() {
					ch <- RoleWant{
						Term: reply.Term,
						Role: Follower,
					}
				}()
				return
			}

			if reply.VoteGranted {
				atomic.AddInt32(&voteCount, 1)
				if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
					go func() {
						ch <- RoleWant{
							Term: currentTerm,
							Role: Leader,
						}
					}()
					return
				}
			}
			wg.Done()
		}(index)
	}
	wg.Wait()
	go func() {
		ch <- RoleWant{
			Term: currentTerm,
			Role: Candidate,
		}
	}()
}
