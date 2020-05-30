package raft

import "sync"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// Invoked by candidates to gather votes (§5.2).
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
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
	DPrintf("[%v][RequestVote]", rf.me)
	// Your code here (2A, 2B).
	currentTerm := rf.getCurrentTerm()

	// 1. Reply false if term < currentTerm
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		DPrintf("[%v][RequestVote] a", rf.me)
		return
	}
	rf.setVotedFor(args.CandidateID)
	reply.VoteGranted = true
	reply.Term = args.Term
	rf.roleWant <- RoleWant{
		Term: args.Term,
		Role: Follower,
	}
	DPrintf("[%v][RequestVote] b", rf.me)
	return

	// TODO: 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
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
	DPrintf("[%v][sendAllRequestVote]", rf.me)
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	rf.setVotedFor(rf.me)
	var wg sync.WaitGroup
	var mu sync.Mutex
	// roleCh := make(chan Role)
	voteCount := 1
	currentTerm := rf.getCurrentTerm()
	maxTerm := currentTerm
	go func() {
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			wg.Add(1)
			go func(server int) {
				args := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: 0,
					LastLogTerm:  0,
				}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, reply)
				DPrintf("[%v][sendAllRequestVote] server:%v args:%v reply:%v", rf.me, server, args, reply)

				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

				mu.Lock()
				if ok && reply.Term > maxTerm {
					maxTerm = reply.Term
					ch <- RoleWant{
						Term: reply.Term,
						Role: Follower,
					}
				}
				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 {
						ch <- RoleWant{
							Term: currentTerm,
							Role: Leader,
						}
					}
				}
				mu.Unlock()
				wg.Done()
			}(index)
		}
		wg.Wait()
		ch <- RoleWant{
			Term: currentTerm,
			Role: Candidate,
		}
	}()

	// select {
	// case role := <-roleCh:
	// 	DPrintf("[%v][sendAllRequestVote] [roleCh:%v]", rf.me, role)
	// 	if role == Follower {
	// 		rf.setCurrentTerm(maxTerm)
	// 	}
	// 	ch <- role
	// 	return
	// }
	//
	// if isFollower {
	// 	DPrintf("[%v][sendAllRequestVote] isFollower", rf.me)
	// 	rf.setCurrentTerm(maxTerm)
	// 	ch <- Follower
	// 	return
	// }
	// if voteCount > len(rf.peers)/2 {
	// 	DPrintf("[%v][sendAllRequestVote] to beLeader", rf.me)
	// 	ch <- Leader
	// 	return
	// }
	// DPrintf("[%v][sendAllRequestVote] to beCandidate", rf.me)
	// ch <- Candidate
}
