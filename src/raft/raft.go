package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Role string

const (
	Follower  Role = "Follwer"
	Candidate      = "Candidate"
	Leader         = "Leader"
)

type RoleWant struct {
	Term int
	Role Role
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role       Role
	roleWantCh chan RoleWant

	applyMsgCh chan ApplyMsg

	quitCh chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	currentTerm int   // latest term server has seen (initialized to 0 on first boot, increases monotonically[单调递增])
	votedFor    int   // candidateId that received vote in current term (or null if none)
	logs        []Log // logs entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest logs entry known to be committed (initialized to 0, increases monotonically[单调递增])
	lastApplied int // index of highest logs entry applied to state machine (initialized to 0, increases monotonically[单调递增])

	// Volatile state on leaders: (Reinitialized after election)
	nextIndexes  []int // for each server, index of the next logs entry to send to that server (initialized to leader last logs index + 1)
	matchIndexes []int // for each server, index of highest logs entry known to be replicated on server (initialized to 0, increases monotonically[单调递增])
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	if rf.role == Leader {
		isleader = true
	}
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) setRole(role Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) incrCurrentTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) setVotedFor(votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = votedFor
}

func (rf *Raft) getLogs() []Log {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.logs
}

func (rf *Raft) setLogs(logs []Log) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = logs
}

func (rf *Raft) changeRole(rw RoleWant) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rw.Term < rf.currentTerm {
		return false
	}
	if rw.Term > rf.currentTerm {
		rf.currentTerm = rw.Term
	}
	if rw.Role != rf.role {
		rf.role = rw.Role
		return true
	}
	return false
}

func (rf *Raft) beFollower() {
	DPrintf("[%v][beFollower] [term:%v]", rf.me, rf.getCurrentTerm())
	for {
		if rf.getRole() != Follower {
			return
		}
		DPrintf("[%v][beFollower+] [term:%v]", rf.me, rf.getCurrentTerm())
		to := getTimeoutByType(AppendEntriesTimeout)
		select {
		case <-rf.quitCh:
			DPrintf("[%v][beFollower] [quitCh]", rf.me)
			return
		case <-to:
			DPrintf("[%v][beFollower] timeout beCandidate", rf.me)
			rf.setRole(Candidate)
			return
		case rw := <-rf.roleWantCh:
			DPrintf("[%v][beFollower-roleWantCh] [rf.getCurrentTerm():%v] [rw:%v]", rf.me, rf.getCurrentTerm(), rw)
			if ok := rf.changeRole(rw); ok {
				return
			}
		}
	}
}

func (rf *Raft) beCandidate() {
	DPrintf("[%v][beCandidate] [term:%v]", rf.me, rf.getCurrentTerm())
	for {
		if rf.getRole() != Candidate {
			return
		}
		rf.incrCurrentTerm()
		DPrintf("[%v][beCandidate+] [term:%v]", rf.me, rf.getCurrentTerm())
		to := getTimeoutByType(ElectionTimeout)
		ch := make(chan RoleWant)
		go rf.sendAllRequestVote(ch)
		select {
		case <-rf.quitCh:
			DPrintf("[%v][beCandidate] [quitCh]", rf.me)
			return
		case <-to:
			DPrintf("[%v][beCandidate] timeout", rf.me)
			rf.setRole(Candidate)
		case rw := <-rf.roleWantCh:
			DPrintf("[%v][beCandidate-roleWantCh] [rf.getCurrentTerm():%v] [rw:%v]", rf.me, rf.getCurrentTerm(), rw)
			if ok := rf.changeRole(rw); ok {
				return
			}
			time.Sleep(time.Millisecond * 200)
		case rw := <-ch:
			DPrintf("[%v][beCandidate-ch] [rf.getCurrentTerm():%v] [rw:%v]", rf.me, rf.getCurrentTerm(), rw)
			if ok := rf.changeRole(rw); ok {
				return
			}
			time.Sleep(time.Millisecond * 200)
		}
	}
}

func (rf *Raft) beLeader() {
	DPrintf("[%v][beLeader] [term:%v]", rf.me, rf.getCurrentTerm())
	rf.mu.Lock()
	for i := range rf.peers {
		rf.nextIndexes[i] = len(rf.logs)
		rf.matchIndexes[i] = 0
	}
	rf.mu.Unlock()
	// ch := make(chan RoleWant)
	// var majority int32 = 1
	go rf.sendAll()
	go rf.checkCommitIndex()
	select {
	case <-rf.quitCh:
		DPrintf("[%v][beLeader] [quitCh]", rf.me)
		return
	case rw := <-rf.roleWantCh:
		DPrintf("[%v][beLeader-roleWantCh] [rf.getCurrentTerm():%v] [rw:%v]", rf.me, rf.getCurrentTerm(), rw)
		if ok := rf.changeRole(rw); ok {
			return
		}
	}
}

func (rf *Raft) checkCommitIndex() {
	for {
		if rf.getRole() != Leader {
			return
		}
		rf.mu.Lock()
		DPrintf("[%v][checkCommitIndex] [nextIndexes:%v]", rf.me, rf.nextIndexes)
		DPrintf("[%v][checkCommitIndex] [matchIndexes:%v]", rf.me, rf.matchIndexes)
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4)
	Outer:
		for n := len(rf.logs) - 1; n >= rf.commitIndex+1; n-- {
			commitCount := 1
			for mi := range rf.matchIndexes {
				if mi == rf.me {
					continue
				}
				if rf.matchIndexes[mi] >= n && rf.logs[n].Term == rf.currentTerm {
					commitCount++
					if commitCount > len(rf.peers)/2 {
						rf.commitIndex = n
						if rf.commitIndex > rf.lastApplied {
							preLastApplied := rf.lastApplied
							commitIndex := rf.commitIndex
							rf.lastApplied = rf.commitIndex
							DPrintf("[%v][sendAllAppendEntries][preLastApplied:%v]", rf.me, preLastApplied)
							go func() {
								for i := preLastApplied + 1; i <= commitIndex; i++ {
									rf.mu.Lock()
									applyMsg := ApplyMsg{
										CommandValid: true,
										Command:      rf.logs[i].Command,
										CommandIndex: i,
									}
									rf.mu.Unlock()
									rf.applyMsgCh <- applyMsg
									DPrintf("[%v][sendApplyMsg] [applymsg:%v]", rf.me, applyMsg)

								}
							}()
						}
						break Outer
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	if rf.role != Leader {
		isLeader = false
	} else {
		index = len(rf.logs)
		term = rf.currentTerm
		rf.logs = append(rf.logs, Log{
			Command: command,
			Term:    term,
			Index:   index,
		})
	}

	DPrintf("[%v][Start] [command:%v] [logs:%v]", rf.me, command, rf.logs)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.quitCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Run() {
	go func() {
		for {
			select {
			case <-rf.quitCh:
				return
			default:
				switch rf.getRole() {
				case Candidate:
					rf.beCandidate()
				case Follower:
					rf.beFollower()
				case Leader:
					rf.beLeader()
				}
			}
		}
	}()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.applyMsgCh = applyCh
	rf.roleWantCh = make(chan RoleWant)
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))
	rf.setVotedFor(-1)
	rf.logs = []Log{{
		Command: nil,
		Term:    -1,
		Index:   0,
	}}
	rf.setCurrentTerm(0)
	rf.quitCh = make(chan struct{})
	rf.Run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
