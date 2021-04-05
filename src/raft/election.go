package raft

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug("Voted requested by %v", args.CandidateId)
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	}
	if rf.votedFor == -1 {
		// Your code here (2A, 2B).
		rf.active <- true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.Debug("Granting vote to %v for term %v", args.CandidateId, args.Term)
	}
	reply.Term = rf.currentTerm
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
func (rf *Raft) callRequestVote(server int, args *RequestVoteArgs) bool {
	reply := &RequestVoteReply{}
	rf.Debug("Requesting vote from %v", server)
	rf.peers[server].Call("Raft.RequestVote", args, reply)
	return reply.VoteGranted
}

func (rf *Raft) callElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.Debug("Starting election for term %v", rf.currentTerm)
	electionTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.state = candidateState

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	votes := 1
	done := false
	rf.mu.Unlock()
	a, b := rf.GetState()
	rf.Debug("After election start: term, state =  %v, %v", a, b)
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.callRequestVote(server, &args)
			if !voteGranted {
				return
			}
			rf.mu.Lock()
			votes++
			rf.Debug("Got vote from %v. total votes = %v", server, votes)
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true
			if rf.currentTerm == electionTerm {
				rf.state = leaderState
				rf.Debug("WIN; becoming leader for term %v", rf.currentTerm)
			}
			rf.mu.Unlock()
			go rf.lead()
			rf.Debug("After election for term %v, state is %v", electionTerm, rf.state)
			//if rf.state == leaderState {
			//	rf.lead()
			//} else {
			//	rf.mu.Unlock()
			//}
		}(server)
	}
}
