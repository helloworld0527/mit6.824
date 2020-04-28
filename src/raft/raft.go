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
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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

type LogEntry struct {

}

type ServiceState int32

const (
	FOLLOWER 	ServiceState = 0
	CANDIDATE 	ServiceState = 1
	LEADER 		ServiceState = 2
)

const ELECTION_TIMEOUT_INTERVAL = 1000 * time.Millisecond
const APPEND_ENTRY_INTERVAL = 180 * time.Millisecond

func randTimeoutDuration() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	baseDuration := ELECTION_TIMEOUT_INTERVAL
	extra := time.Duration(r.Int63()) % baseDuration
	return time.Duration(baseDuration + extra)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          	// Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd 	// RPC end points of all peers
	persister *Persister          	// Object to hold this peer's persisted state
	me        int                 	// this peer's index into peers[]
	dead      int32               	// set by Kill()

	// Your data here (2A, 2B, 2C).
	// state a Raft server must maintain.

	state 			ServiceState	// FOLLOWER, CANDIDATE, LEADER
	electionTimer	*time.Timer		// timer for election timeout
	shutdown		chan struct{}	// notify threads when the server is killed

	// persistent state
	currentTerm int					// server term
	votedFor 	int					// one term only vote for one server
	logs		[]LogEntry

	commitIndex int
	lastApplied int

	nextIndex 	[]int
	matchIndex	[]int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int 	// candidate's term
	CandidateId 	int		// candidate requesting vote
	LastLogIndex 	int		// index of candidate's last committed log entry
	LastLogTerm		int 	// term of candidate's last committed log entry
}

//
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int		// current term, for candidate to update itself
	VoteGranted 	bool	// true means vote
}

type AppendEntriesArgs struct {
	Term 			int 	// leader's term
	LeaderId		int
	PrevLogIndex	int 	// index of log entry immediately preceding new ones
	PrevLogTerm		int
	Entries 		[]LogEntry
	LeaderCommit	int 	// leader's commitIndex
}

type AppendEntriesReply struct {
	Term 			int  	// for leader to update itself
	Success			bool 	// true if follower contained entry matching prevLogIndex and term
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
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	voteGranted := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		voteGranted = false								// refuse this RPC
	} else {
		rf.resetElectionTimer(randTimeoutDuration())	// receive valid RPC from candidate
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
		if rf.votedFor != -1 {
			voteGranted = false							// has voted another server in this term
		} else {
			rf.votedFor = args.CandidateId				// vote
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
}

func (rf *Raft) launchElection() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	timeoutDuration := randTimeoutDuration()
	rf.resetElectionTimer(timeoutDuration)
	timer := time.After(timeoutDuration) 	// if timeout, end this election
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		0,
		0,
	}
	voteCount := 1
	done := false
	DPrintf("[%d] launch an election at term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	replyCh := make(chan int, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			reply := RequestVoteReply{}
			DPrintf("[%d] send request vote to %d at term %d", rf.me, serverId, args.Term)
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok { 	// request didn't reach the server
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if done {	// election has finished
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.resetElectionTimer(randTimeoutDuration())
				return
			}
			if reply.VoteGranted {
				voteCount++
			}
			replyCh <- 1
		}(i)
	}

	majorityNum := len(rf.peers) / 2 + 1
	// vote >= threshold || state == follower || timeout will end this loop
	for voteCount < majorityNum && rf.state == CANDIDATE {
		select {
		case <- timer:
			return
		case <- rf.shutdown:
			return
		case <- replyCh: 	// receive a vote request reply
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	done = true
	if voteCount >= majorityNum && rf.state == CANDIDATE { // win
		rf.state = LEADER
		go rf.ticktock()
		DPrintf("[%d] become the leader of term %d", rf.me, rf.currentTerm)
	}
}

// leader send appendEntry RPC
func (rf *Raft) ticktock() {
	timer := time.NewTimer(APPEND_ENTRY_INTERVAL)
	for {
		select {
		case <- timer.C:
			if _, isLeader := rf.GetState(); isLeader {
				go rf.sendAppendEntries()
				timer.Reset(APPEND_ENTRY_INTERVAL) 	// timer has expired, so don't need call stop()
			} else {
				return 								// not a leader anymore, stop send appendEntry RPC
			}
		case <-rf.shutdown:
			return
		}
	}
}

func (rf *Raft) sendAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{}
		rf.mu.Lock()
		args.Term = rf.currentTerm
		rf.mu.Unlock()
		args.LeaderId = rf.me
		args.Entries = nil
		go func(serverId int) {
			reply := AppendEntriesReply{}
			DPrintf("[%d] leader send heartbeat to %d", args.LeaderId, serverId)
			ok := rf.peers[serverId].Call("Raft.HandleAppendEntries", &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.resetElectionTimer(randTimeoutDuration())
				return
			}
		}(i)
	}
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	success := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		success = false
	} else {
		rf.resetElectionTimer(randTimeoutDuration()) 	// receive valid RPC from leader
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = success
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.shutdown)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(randTimeoutDuration())
	rf.shutdown = make(chan struct{})
	DPrintf("[%d] initiate a raft server", me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <- rf.electionTimer.C:
				if _, isLeader := rf.GetState(); !isLeader {
					go rf.launchElection()
				}
			case <-rf.shutdown:
				return
			}
		}
	}()

	return rf
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {
	// Always stop a electionTimer before reusing it. See https://golang.org/pkg/time/#Timer.Reset
	// We ignore the value return from Stop() because if Stop() return false, the value inside the channel has been drained out
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}
