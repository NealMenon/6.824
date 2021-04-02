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
	"math/rand"
	//"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "../labrpc"

const (
	leaderState    = 0
	followerState  = 1
	candidateState = 2
)

type State int

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

// Log Entry
type LogEntry struct {
	Term    int
	Command []byte
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// Arguments while attempting AppendEntry RPC
type AppendEntryArgs struct {
	Term             int
	LeaderID         int
	PreviousLogIndex int
	PreviousLogTerm  int
	LogEntry         []byte
	Index            int
}

// Reply from attempting AppendEntry RPC
type AppendEntryReply struct {
	Term    int
	Success bool
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent  on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile on all servers
	commitIndex int
	lastApplied int
	state       State
	active      chan bool // meant for heartbeat

	// volatile on leaders
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rand.Seed(time.Now().UnixNano())
		timeout := time.Duration(rand.Intn(150)+100) * time.Millisecond
		select {
		case <-rf.active:
			rf.Debug("Still alive")
		case <-time.After(timeout):
			rf.Debug("Timed out, starting election")
			rf.callElection()
		}
		//time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) appendEntry() {
	for {
		//time.Sleep(time.Duration(rand.Intn(150)+50) * time.Millisecond)
		rf.active <- true
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = followerState
	rf.active = make(chan bool)

	rf.nextIndex = make([]int, 8)
	rf.matchIndex = make([]int, 8)

	// Your initialization code here (2A, 2B, 2C).
	go rf.heartbeat()
	//go rf.appendEntry()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
