package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// Invoked by leader to replicate logs entries (§5.3); also used as heartbeat (§5.2).
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if logs doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the logs
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of logs entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // logs entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%v][AppendEntries]", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[%v][AppendEntries] 1.", rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
	}
	reply.Success = true
	go func() {
		rf.roleWantCh <- RoleWant{
			Term: args.Term,
			Role: Follower,
		}
	}()

	// TODO: 2. Reply false if logs doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > len(rf.logs)-1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = args.Term
		reply.Success = false
		go func() {
			rf.roleWantCh <- RoleWant{
				Term: args.Term,
				Role: Follower,
			}
		}()
		DPrintf("[%v][AppendEntries] 2.", rf.me)
		return
	}
	// TODO: 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	n := len(rf.logs)
	for i, entry := range args.Entries {
		if index := args.PrevLogIndex + i + 1; index < len(rf.logs) {
			if rf.logs[index].Term != entry.Term {
				n = index
				break
			}
		} else {
			break
		}
	}
	DPrintf("[%v][AppendEntries] 3.", rf.me)

	// TODO: 4. Append any new entries not already in the logs
	rf.logs = append(rf.logs[:n], args.Entries...)
	DPrintf("[%v][AppendEntries] 4.", rf.me)

	// TODO: 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.logs)-1)
		if rf.commitIndex > rf.lastApplied {
			preLastApplied := rf.lastApplied
			commitIndex := rf.commitIndex
			rf.lastApplied = rf.commitIndex
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
	}

	reply.Term = args.Term
	reply.Success = true
	go func() {
		rf.roleWantCh <- RoleWant{
			Term: args.Term,
			Role: Follower,
		}
	}()
	DPrintf("[%v][AppendEntries] 5.", rf.me)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAll() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			for {
				if rf.getRole() != Leader {
					return
				}
				rf.mu.Lock()
				nextIndex := rf.nextIndexes[server]
				end := len(rf.logs)
				if e := nextIndex + 10; e < end {
					end = e
				}
				entries := rf.logs[nextIndex:end]
				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.logs[nextIndex-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{
					Term:    0,
					Success: false,
				}
				DPrintf("[%v][sendAllAppendEntries] [server:%v]", rf.me, server)
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(server, &args, &reply)

				// 1. If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if ok && reply.Term > currentTerm {
					DPrintf("[%v][sendAllAppendEntries][server:%v] 1.", rf.me, server)
					go func() {
						rf.roleWantCh <- RoleWant{
							Term: reply.Term,
							Role: Follower,
						}
					}()
					return
				}
				if ok {
					// TODO: If successful: update nextIndex and matchIndex for follower (§5.3)
					// TODO: If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
					if reply.Success {
						if len(entries) > 0 {
							rf.mu.Lock()
							rf.nextIndexes[server] = nextIndex + len(entries)
							rf.matchIndexes[server] = nextIndex + len(entries) - 1
							rf.mu.Unlock()
						}
						time.Sleep(time.Millisecond * 200)
					} else {
						rf.mu.Lock()
						// ni := 1
						// if t := rf.nextIndexes[server] - 5; t > 1 {
						// 	ni = t
						// }
						rf.nextIndexes[server]--
						rf.mu.Unlock()
						time.Sleep(time.Millisecond * 20)
					}
				}
			}
		}(index)
	}
}

func (rf *Raft) sendAllAppendEntries(ch chan RoleWant, majority *int32) {
	rf.mu.Lock()
	var wg sync.WaitGroup
	DPrintf("[%v][sendAllAppendEntries] [role:%v] [term:%v]", rf.me, rf.role, rf.currentTerm)
	currentTerm := rf.currentTerm
	nextIndexes := rf.nextIndexes
	logs := rf.logs
	commitIndex := rf.commitIndex
	me := rf.me
	rf.mu.Unlock()
	for index := range rf.peers {
		if index == me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			// TODO: If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			rf.mu.Lock()
			if rf.role != Leader {
				wg.Done()
				return
			}
			nextIndex := nextIndexes[server]
			end := len(logs)
			if e := nextIndex + 10; e < end {
				end = e
			}
			entries := logs[nextIndex:end]
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  logs[nextIndex-1].Term,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			reply := AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			rf.mu.Unlock()
			DPrintf("[%v][sendAllAppendEntries] [server:%v]", me, server)
			ok := rf.sendAppendEntries(server, &args, &reply)
			// 1. If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			if ok && reply.Term > currentTerm {
				DPrintf("[%v][sendAllAppendEntries][server:%v] 1.", rf.me, server)
				go func() {
					rf.roleWantCh <- RoleWant{
						Term: reply.Term,
						Role: Follower,
					}
				}()
				return
			}
			if ok {
				rf.mu.Lock()
				// TODO: If successful: update nextIndex and matchIndex for follower (§5.3)
				// TODO: If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
				if reply.Success {
					rf.nextIndexes[server] = nextIndex + len(entries)
					rf.matchIndexes[server] = nextIndex + len(entries) - 1
				} else {
					ni := 1
					if t := rf.nextIndexes[server] - 5; t > 1 {
						ni = t
					}
					rf.nextIndexes[server] = ni
				}
				rf.mu.Unlock()

				atomic.AddInt32(majority, 1)
			}
			// if ok {
			// 	atomic.AddInt32(&count, 1)
			// 	if atomic.LoadInt32(&count) > int32(len(rf.logs)/2) {
			// 		go func() {
			// 			ch <- RoleWant{
			// 				Term: currentTerm,
			// 				Role: Leader,
			// 			}
			// 		}()
			// 		return
			// 	}
			// }
			if ok {
				rf.mu.Lock()
				// TODO: If successful: update nextIndex and matchIndex for follower (§5.3)
				// TODO: If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
				if reply.Success {
					rf.nextIndexes[server] = nextIndex + len(entries)
					rf.matchIndexes[server] = nextIndex + len(entries) - 1
					DPrintf("[%v][sendAllAppendEntries][server:%v] [matchIndexes:%v]", rf.me, server, rf.matchIndexes)
				} else {
					rf.nextIndexes[server]--
				}
				rf.mu.Unlock()

				atomic.AddInt32(majority, 1)
			}
			wg.Done()
			DPrintf("[%v][sendAllAppendEntries][server:%v] done", rf.me, server)
		}(index)
	}

	wg.Wait()
	DPrintf("[%v][sendAllAppendEntries] wait", rf.me)
}
