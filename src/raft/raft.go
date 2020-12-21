package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	HEARTBEATINTERVAL = 50
	ELECTIONINTERVAL  = 150
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	myState int //0 follower 1 candidate 2 leader
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// voteChan	chan bool
	// heartBeat	chan bool
	voteCount int
	timer     *time.Timer
	applyCh   chan ApplyMsg
	leaderCh  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.myState == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	VoteGranted bool
	Term        int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("server %d get a vote RPC\n", rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// return
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			// reply.VoteGranted = true
			rf.myState = FOLLOWER
			// rf.votedFor = args.CandidateId
			rf.votedFor = -1 //why = -1?
			// rf.timer.Reset(getRandomInterval())
		}
		reply.VoteGranted = false
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //why candidateID?
			lastIndex := rf.getLastIndex()
			lastTerm := rf.getLastTerm()
			// fmt.Printf("server %d: lastTerm=%d args.lastterm=%d  lastIndex=%d args.index=%d\n", rf.me, lastTerm, args.LastLogTerm, lastIndex, args.LastLogIndex)
			if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.myState = FOLLOWER
				rf.timer.Reset(getRandomInterval())
			}
		}
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVoteRPC() {
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	// fmt.Printf("server %d broadcasts voteRPCs\n", rf.me)
	for i := range rf.peers {
		if i != rf.me && rf.myState == CANDIDATE {
			go func(server int) {
				var reply RequestVoteReply

				// if branch??
				rf.sendRequestVote(server, args, &reply)

				if reply.VoteGranted == true {
					rf.mu.Lock()
					rf.voteCount++
					if rf.voteCount > len(rf.peers)/2 {
						rf.leaderCh <- true
					}
					rf.mu.Unlock()
				} else {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.myState = FOLLOWER
						rf.votedFor = -1
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) getLastIndex() int {
	len := len(rf.log)
	if len == 0 {
		return 0
	} else {
		return rf.log[len-1].Index
	}
}

func (rf *Raft) getLastTerm() int {
	len := len(rf.log)
	if len == 0 {
		return 0
	} else {
		return rf.log[len-1].Term
	}
}

type AppendEntriesArgs struct {
	// machine state
	Term     int
	LeaderId int
	// log state
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int //improve the index searching
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	// fmt.Printf("server %d gets a RPC from %d\n", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			// reply.Success = true
			rf.myState = FOLLOWER
			// rf.votedFor = -1 //for what?
		}
		reply.Term = args.Term
		rf.timer.Reset(getRandomInterval()) //whether we need reset timer for first if branch?
		// fmt.Printf("server %d reset timer\n", rf.me)
		lastIndex := rf.getLastIndex()
		if args.PrevLogIndex > lastIndex {
			reply.NextIndex = lastIndex + 1
		} else {
			baseIndex := rf.log[0].Index
			// fmt.Printf("server %d baseIndex = %d\n", rf.me, baseIndex)
			if args.PrevLogIndex >= baseIndex {
				term := rf.log[args.PrevLogIndex-baseIndex].Term

				if args.PrevLogTerm != term {
					// fmt.Printf("follower server %d delete log  args.prevlogindex=%d baseindex=%d\n", rf.me, args.PrevLogIndex, baseIndex)
					for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
						// fmt.Printf("log[%d].term=%d term=%d\n", i-baseIndex, rf.log[i-baseIndex].Term, term)
						if rf.log[i-baseIndex].Term != term {
							reply.NextIndex = i + 1
							reply.Term = rf.currentTerm //for what? reply.Term is meaningless on this condition
							return
						}
					}
				} else { //copy the LogEntries
					// fmt.Printf("follower server %d copy the log of leader %d\n", rf.me, args.LeaderId)
					rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
					rf.log = append(rf.log, args.Entries...)
					lastIndex := rf.getLastIndex()
					reply.NextIndex = lastIndex + 1
					reply.Success = true
				}
			}
			/*if args.PrevLogIndex > baseIndex {
				term := rf.log[args.PrevLogIndex-baseIndex].Term
				if args.PrevLogTerm != term {
					for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
						if rf.log[i-baseIndex].Term != term {
							reply.NextIndex = i + 1
							break
						}
					}
					return
				}
			}
			if args.PrevLogIndex < baseIndex {

			} else {
				// 添加log中没有的新entries
				rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
				rf.log = append(rf.log, args.Entries...)
				reply.Success = true
				reply.NextIndex = rf.getLastIndex() + 1
			}*/

			if args.LeaderCommit > rf.commitIndex {
				lastIndex := rf.getLastIndex()
				if args.LeaderCommit < lastIndex {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = lastIndex
				}
				for rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
					entry := rf.log[rf.lastApplied]
					msg := ApplyMsg{
						Index:   entry.Index,
						Command: entry.Command,
					}
					// fmt.Printf("follower server %d commit logindex %d Command %v\n", rf.me, entry.Index, entry.Command)
					rf.applyCh <- msg
				}
			}
		}
	}

	// rf.timer.Reset(getRandomInterval()) //whether we need reset timer for first if branch?
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntriesRPC() {
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me { // LEADER??
			go func(i int) { //server
				args.PrevLogTerm = -1
				args.PrevLogIndex = rf.nextIndex[i] - 1 //why -1
				if args.PrevLogIndex > -1 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}
				args.Entries = make([]LogEntry, 0)
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]:]...)

				var reply AppendEntriesReply
				// fmt.Printf("server %d send heartbeat to %d\n", rf.me, i)

				if rf.sendAppendEntries(i, args, &reply) { //server
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.myState = FOLLOWER
						rf.votedFor = -1
						return
					}
					// if rf.myState != LEADER {
					// 	return
					// } //why need this?
					// fmt.Printf("server %d RPC reply returns %t\n", rf.me, reply.Success)
					if reply.Success == true {
						// update nextIndex and matchIndex, refer to section 5.3
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						// fmt.Printf("server %d in success branch\n", rf.me)
						//commit
						N := rf.commitIndex
						lastIndex := rf.getLastIndex()
						// fmt.Printf("server %d LastIndex=%d\n", rf.me, lastIndex)
						for i := rf.commitIndex + 1; i <= lastIndex; i++ {
							count := 1
							for j := range rf.peers {
								if j != rf.me && rf.matchIndex[j] >= i {
									count++
								}
							}
							if count > len(rf.peers)/2 {
								N = i
							}
						}
						// fmt.Printf("leader server %d N=%d  commitIndex=%d\n", rf.me, N, rf.commitIndex)
						if N > rf.commitIndex { //&& rf.log[N].Term == rf.currentTerm 		for what??
							// fmt.Printf("server %d commit logs\n", rf.me)
							rf.commitIndex = N
							for rf.commitIndex > rf.lastApplied {
								rf.lastApplied++
								entry := rf.log[rf.lastApplied]
								msg := ApplyMsg{
									Index:   entry.Index,
									Command: entry.Command,
								}
								rf.applyCh <- msg
								// fmt.Printf("leader server %d command %v\n", rf.me, entry.Command)
							}
						}
					} else {
						rf.nextIndex[i] = reply.NextIndex
						// fmt.Printf("server %d in unsuccesss branch  nexiindex=%d\n", rf.me, reply.NextIndex)
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) startElection() {
	// fmt.Printf("server %d is going to start Election!\n", rf.me)
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.myState = CANDIDATE
	rf.mu.Unlock()
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me

	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()

	rf.broadcastRequestVoteRPC()
	// fmt.Printf("server %d gets %d votes!\n", rf.me, rf.voteCount)
}

func getRandomInterval() time.Duration {
	return time.Duration(rand.Intn(ELECTIONINTERVAL)+ELECTIONINTERVAL) * time.Millisecond
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()
	if isLeader == true {
		rf.mu.Lock()
		lastIndex := rf.getLastIndex()
		index = lastIndex + 1
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.votedFor = -1
	rf.myState = FOLLOWER
	rf.currentTerm = 0
	rf.timer = time.NewTimer(getRandomInterval())

	rf.applyCh = applyCh
	rf.leaderCh = make(chan bool, 10)
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			// fmt.Printf("Here is server %d state=%d\n", rf.me, rf.myState)
			switch rf.myState {
			case FOLLOWER:
				select {
				case <-rf.timer.C:
					rf.startElection()
					rf.mu.Lock()
					rf.timer.Reset(getRandomInterval())
					rf.mu.Unlock()
				}
			case CANDIDATE:
				select {
				case <-rf.timer.C:
					rf.startElection()
					rf.mu.Lock()
					rf.timer.Reset(getRandomInterval())
					rf.mu.Unlock()
					// default:
				case <-rf.leaderCh:
					// fmt.Printf("candidate server %d has %d votes\n", rf.me, rf.voteCount)
					rf.mu.Lock()
					if rf.voteCount > len(rf.peers)/2 {
						// fmt.Printf("candidate server %d has %d votes and becomes leader\n", rf.me, rf.voteCount)
						rf.myState = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						lastIndex := rf.getLastIndex()
						for i := range rf.peers {
							rf.nextIndex[i] = lastIndex + 1
						}
					}
					rf.mu.Unlock()
				}
			case LEADER:
				// fmt.Printf("leader server %d sends appendRPC to all\n", rf.me)
				rf.broadcastAppendEntriesRPC()
				time.Sleep(HEARTBEATINTERVAL * time.Millisecond)
			default:
				fmt.Println("Unknown Condition!")
			}
		}
	}()

	return rf
}
