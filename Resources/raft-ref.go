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
	"sync"
    "sync/atomic"
	"time"
)
//import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	leaderState    = 0
	followerState  = 1
	candidateState = 2
)

//const (
//	voteTag      = "VOTE"
//	electionTag  = "ELECTION"
//	winnerTag    = "WINNER"
//	resultTag    = "RESULT"
//	appendActTag = "APPEND_ACT"
//)

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
	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.

	// persistent  on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile on all servers
	commitIndex      int
	lastApplied      int
	state            State
	timeOfLastAppend time.Time

	// volatile on leaders
	nextIndex  []int
	matchIndex []int
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

	// Your initialization code here (2A, 2B, 2C)

	DPrintf("Making %v", me)

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = followerState
	rf.timeOfLastAppend = time.Now()

	go rf.checkActive()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) checkActive() {
	for {
		//DPrintf("Setting new electionTimeout for %v", rf.me)
		electionTimeout := time.Duration(150 + rand.Intn(200))
		for {
			rf.mu.Lock()
			timer := rf.timeOfLastAppend
			rf.mu.Unlock()
			if time.Since(timer) > (electionTimeout*time.Millisecond) && !rf.killed() {
				DRaftf("Activity timed out in term %v at %v", rf.me, rf.currentTerm, rf.timeOfLastAppend)
				rf.attemptElection()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

//
// RPC handler for leader to call AppendEntry on listening followers
//
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs) bool {
	//DRaftf("SEND from AE to %v term %v", rf.me, server, rf.currentTerm)
	reply := &AppendEntryReply{}
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	//if !rf.killed() {
	//	return
	//}
	//if !reply.Success {
	//	DRaftf("AE FAILURE", appendActTag)
	//	time.Sleep(100 * time.Millisecond)
	//}
	if !ok {
		DPrintf("\t[ACTIVITY]\tAppendEntry from %v took too long or failed", server)
		return false
	}
	return true
}

// RPC call to AppendEntry on follower
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//DRaftf("RECV from %v to %v term %v", appendActTag, args.LeaderID, rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DRaftf("ERROR AE from OLD LEADER %v to %v term %v real term %v; IGNORING", appendActTag, args.LeaderID, rf.me, args.Term, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	reply.Success = true
	rf.timeOfLastAppend = time.Now()
	rf.state = followerState
	rf.currentTerm = args.Term
	rf.votedFor = -1
	/* TODO update for 2B */
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) bool {
	//return true
	reply := &RequestVoteReply{}
	//DRaftf("Vote request from %v to %v for term %v", electionTag, rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DRaftf("Vote not received in time from %v for term %v", rf.me, server, rf.currentTerm)

	}
	return reply.VoteGranted
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("In RequestVote of %v from %v", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm >= args.Term {
		DRaftf("Outdated Election request from %v for term %v ", rf.me, args.CandidateId, args.Term)
		return
	}
	if rf.votedFor == -1 /* || rf.votedFor == args.CandidateId  && args.LastLogIndex >= rf.commitIndex*/ {
		rf.timeOfLastAppend = time.Now()
		//DRaftf("Votes for %v from %v term %v, at %v", voteTag, args.CandidateId, rf.me, args.Term, rf.timeOfLastAppend)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DRaftf("Voting for %v for term %v", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		DRaftf("Already voted for %v for term %v", rf.me, rf.votedFor, args.Term)
	}
}

func (rf *Raft) attemptElection() {
	rf.mu.Lock()
	rf.state = candidateState
	rf.currentTerm++
	rf.votedFor = rf.me
	votes := 1
	electionTerm := rf.currentTerm
	rf.timeOfLastAppend = time.Now()
	//electionStart := rf.timeOfLastAppend

	DRaftf("Starting election for term %v", rf.me, rf.currentTerm)

	voteArg := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  rf.lastApplied,
	}
	done := false
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		//DPrintf("out I is %v", i)
		go func(server int) {
			//DPrintf("Attempting to collect vote from %v", server)
			voteGranted := rf.sendRequestVote(server, voteArg)
			if !voteGranted {
				DRaftf("Didn't get vote from %v", rf.me, server)
				return
			}
			DRaftf("Received vote from %v for term %v", rf.me, server, electionTerm)
			rf.mu.Lock()
			votes++
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true
			if rf.state != candidateState || rf.currentTerm != electionTerm {
				// failed
				return
			}
			rf.mu.Unlock()
			DRaftf("Becoming leader", rf.me)
			rf.winElection()
		}(i)
	}
	//DRaftf("LOST ELECTION", rf.me)

	//for i := 0; i < len(rf.peers); i++ {
	//	if i == rf.me {
	//		continue
	//	}
	//	voteReply := RequestVoteReply{}
	//	voteReplies = append(voteReplies, &voteReply)
	//	//DPrintf("[ELECTION]\t%v req vote from %v", rf.me, i)
	//	go rf.sendRequestVote(i, &voteArg, &voteReply)
	//}
	//time.Sleep(15 * time.Millisecond) // wait for response
	//for {
	//	for from, rep := range voteReplies {
	//		//DPrintf("Reply: %v", rep)
	//		if rep.VoteGranted {
	//			votes++
	//			DRaftf("\t Received vote %v from %v for term %v", electionTag, rf.me, from, rf.currentTerm)
	//			rep.VoteGranted = false
	//		}
	//		if rep.Term > rf.currentTerm {
	//			break
	//		}
	//	}
	//	//if votes > len(rf.peers)/2 {
	//	//	break
	//	//}
	//	if time.Since(electionStart) > (90 * time.Millisecond) {
	//		//DPrintf("[ELECTION]\t%v's Election took too long for term %v", rf.me, rf.currentTerm)
	//		DRaftf("%v's Election took too long for term %v", electionTag, rf.me, rf.currentTerm)
	//		break
	//	}
	//	if rf.state != candidateState {
	//		//DPrintf("[ELECTION]\tSomeone else won the election")
	//		DRaftf("Someone else won the election", electionTag)
	//		rf.votedFor = -1
	//		break
	//	}
	//}
	//if rf.state != candidateState {
	//	rf.loseElection()
	//	DPrintf("[LOSER]\t%v has lost the election for term %v, state is %v", rf.me, rf.currentTerm, rf.state)
	//}
	//DPrintf("[RESULT]\tVotes won by %v in term %v: %v", rf.me, rf.currentTerm, votes)
	//DRaftf("Votes won by %v for term %v: %v votes", resultTag, rf.me, rf.currentTerm, votes)
	//if votes > len(rf.peers)/2 && rf.state == candidateState {
	//	DPrintf("[WINNER]\t%v has won the election for term %v", rf.me, rf.currentTerm)
	//	rf.winElection()
	//} else {
	//	rf.loseElection()
	//	DPrintf("[LOSER]\t%v has lost the election for term %v, state is %v", rf.me, rf.currentTerm, rf.state)
	//}
}

// this warning typically arises if code re-uses the same RPC reply
// variable for multiple RPC calls, or if code restores persisted
// state into variable that already have non-default values.

func (rf *Raft) winElection() {
	// first set self as leader
	rf.mu.Lock()
	//DPrintf("\t%v has won election for term %v", rf.me, rf.currentTerm)
	DRaftf("[WINNER] of election for term %v", rf.me, rf.currentTerm)
	rf.timeOfLastAppend = time.Now()
	rf.state = leaderState
	appEndArg := AppendEntryArgs{
		Term:             rf.currentTerm,
		LeaderID:         rf.me,
		PreviousLogIndex: rf.commitIndex,
	}

	rf.mu.Unlock()
	//appEndReplies := make([]*AppendEntryReply, 0)
	//timeCatcher := time.Now()
	aessent := 0

	for !rf.killed() {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				success := rf.sendAppendEntry(server, &appEndArg)
				if !success {
					DRaftf("Failure to AE from %v term %v", rf.me, i, rf.currentTerm)
				}
			}(i)
			aessent++
		}
		time.Sleep(90 * time.Millisecond)
		//if time.Since(timeCatcher) > time.Second {
		//	DRaftf("AEs Sent: %v", rf.me, aessent)
		//	timeCatcher = time.Now()
		//}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == leaderState
	//DPrintf("Status: %v, %v")
	return term, isleader
}

//func (rf *Raft) loseElection() {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	rf.state = 1
//	rf.timeOfLastAppend = time.Now()
//}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

//func (rf *Raft) startElection() {
//	votes := 1
//	rf.mu.Lock()
//	rf.votedFor = rf.me
//	rf.currentTerm++
//	rf.state = candidateState
//	rf.timeOfLastAppend = time.Now()
//	electionStart := rf.timeOfLastAppend
//
//	DRaftf("%v starting election for term %v", electionTag, rf.me, rf.currentTerm)
//
//	voteArg := RequestVoteArgs{}
//	voteReplies := make([]*RequestVoteReply, 0)
//
//	voteArg.Term = rf.currentTerm
//	voteArg.CandidateId = rf.me
//	voteArg.LastLogIndex = rf.commitIndex
//	voteArg.LastLogTerm = rf.lastApplied
//
//	//DPrintf("New election called by %v", rf.me)
//	for i := 0; i < len(rf.peers); i++ {
//		if i == rf.me {
//			continue
//		}
//		voteReply := RequestVoteReply{}
//		voteReplies = append(voteReplies, &voteReply)
//		//DPrintf("[ELECTION]\t%v req vote from %v", rf.me, i)
//		go rf.sendRequestVote(i, &voteArg, &voteReply)
//	}
//	time.Sleep(15 * time.Millisecond) // wait for response
//	for {
//		for from, rep := range voteReplies {
//			//DPrintf("Reply: %v", rep)
//			if rep.VoteGranted {
//				votes++
//				DRaftf("\t Received vote %v from %v for term %v", electionTag, rf.me, from, rf.currentTerm)
//				rep.VoteGranted = false
//			}
//			if rep.Term > rf.currentTerm {
//				break
//			}
//		}
//		//if votes > len(rf.peers)/2 {
//		//	break
//		//}
//		if time.Since(electionStart) > (90 * time.Millisecond) {
//			//DPrintf("[ELECTION]\t%v's Election took too long for term %v", rf.me, rf.currentTerm)
//			DRaftf("%v's Election took too long for term %v", electionTag, rf.me, rf.currentTerm)
//			break
//		}
//		if rf.state != candidateState {
//			//DPrintf("[ELECTION]\tSomeone else won the election")
//			DRaftf("Someone else won the election", electionTag)
//			rf.votedFor = -1
//			break
//		}
//	}
//	if rf.state != candidateState {
//		rf.loseElection()
//		DPrintf("[LOSER]\t%v has lost the election for term %v, state is %v", rf.me, rf.currentTerm, rf.state)
//	}
//	DPrintf("[RESULT]\tVotes won by %v in term %v: %v", rf.me, rf.currentTerm, votes)
//	rf.mu.Unlock()
//	DRaftf("Votes won by %v for term %v: %v votes", resultTag, rf.me, rf.currentTerm, votes)
//	if votes > len(rf.peers)/2 && rf.state == candidateState {
//		DPrintf("[WINNER]\t%v has won the election for term %v", rf.me, rf.currentTerm)
//		rf.winElection()
//	} else {
//		rf.loseElection()
//		DPrintf("[LOSER]\t%v has lost the election for term %v, state is %v", rf.me, rf.currentTerm, rf.state)
//	}
//}
