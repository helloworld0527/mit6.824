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
	"bytes"
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

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
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
	CommandTerm		int
}

// Log index and term can determine a unique log entry
type LogEntry struct {
	LogIndex	int
	LogTerm		int
	Command 	interface{}
}

type ServiceState int32

const (
	FOLLOWER 	ServiceState = 0
	CANDIDATE 	ServiceState = 1
	LEADER 		ServiceState = 2
)

const ElectionTimeoutInterval = 1000 * time.Millisecond
const AppendEntryInterval = 180 * time.Millisecond

func randTimeoutDuration() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	baseDuration := ElectionTimeoutInterval
	extra := time.Duration(r.Int63()) % baseDuration
	return baseDuration + extra
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          		// Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd 		// RPC end points of all peers
	persister *Persister          		// Object to hold this peer's persisted state
	me        int                 		// this peer's index into peers[]
	dead      int32               		// set by Kill()

	// state a Raft server must maintain.
	state 			ServiceState		// FOLLOWER, CANDIDATE, LEADER
	electionTimer	*time.Timer			// timer for election timeout
	shutdown		chan struct{}		// notify threads when the server is killed
	applyCh			chan ApplyMsg		// apply to the client
	applyNotify		chan struct{} 		// inform the routine to apply command to service

	// persistent state
	currentTerm 	int					// server term
	votedFor 		int					// one term only vote for one server
	logs			[]LogEntry
	lastLogIndex 	int					// index of the last log

	// for all servers (volatile)
	currentLeader	int 				// current leader id in this term
	commitIndex 	int					// highest index to be committed
	lastApplied 	int					// highest index applied to state machine

	// for leader (volatile)
	nextIndex 		[]int				// for each server, index of the next log entry to send to the server
	matchIndex		[]int				// index of each server's last effective log

}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// get current leader id
//
func (rf *Raft) GetLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// call persist() before any method returns, who modifies persistent states.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.logs) != nil ||
		e.Encode(rf.lastLogIndex) != nil{
		DPrintf("(info) Persis encode error")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var lastLogIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastLogIndex) != nil{
		DPrintf("(info) Persis decode error")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = voteFor
	  rf.logs = logs
	  rf.lastLogIndex = lastLogIndex
	}
}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters
//
type RequestVoteArgs struct {
	Term 			int 	// candidate's term
	CandidateId 	int		// candidate requesting vote
	LastLogIndex 	int		// index of candidate's last committed log entry
	LastLogTerm		int 	// term of candidate's last committed log entry
}

//
// field names must start with capital letters!
//
type RequestVoteReply struct {
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
	LeaderTerm		int		// the leader's term when this RPC was sent out
	HintIndex		int		// the next index the follower wants to receive
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
// RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	voteGranted := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		voteGranted = false									// refuse this RPC
	} else {
		if rf.currentTerm < args.Term {						// if current term is early, update
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
		currentLastLogTerm := rf.logs[rf.lastLogIndex].LogTerm
		if rf.votedFor != -1 || args.LastLogTerm < currentLastLogTerm{
			voteGranted = false								// has voted another server in this term or is not at least as up-to-date as receiver's log
		} else if args.LastLogTerm == currentLastLogTerm && args.LastLogIndex < rf.lastLogIndex{
			voteGranted = false
		} else {
			// Note:the RPC may come from a server whose logs are out of date,
			// if we reset the timer as long as it has higher current term,
			// it may prevent the real qualified follower becoming candidate
			rf.resetElectionTimer(randTimeoutDuration())	// receive valid RPC from candidate
			rf.votedFor = args.CandidateId					// vote
		}
	}

	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
}

func (rf *Raft) launchElection() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	timeoutDuration := randTimeoutDuration()
	rf.resetElectionTimer(timeoutDuration)
	timer := time.After(timeoutDuration) 	// if timeout, end this election
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.lastLogIndex,
		rf.logs[rf.lastLogIndex].LogTerm,
	}
	voteCount := 1
	done := false
	DPrintf("(info) (%d) launch an election at term[%d]", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	replyCh := make(chan int, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			reply := RequestVoteReply{}
			DPrintf("(info) (%d) send request vote to (%d) at term[%d]", rf.me, serverId, args.Term)
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok || done { 	// request didn't reach the server or election has finished
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
		case <- timer:		// election timeout, end this election
			return
		case <- rf.shutdown:
			return
		case <- replyCh: 	// receive a vote request reply, then to judge the condition
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	done = true
	if voteCount >= majorityNum && rf.state == CANDIDATE { // win
		rf.state = LEADER
		rf.initIndex()		// reinitialize the nextIndex and matchIndex
		go rf.ticktock()
		DPrintf("(info) (%d) become the leader of term[%d]", rf.me, rf.currentTerm)
	}
}

//
// leader send appendEntry RPC
//
func (rf *Raft) ticktock() {
	timer := time.NewTimer(AppendEntryInterval)
	for {
		select {
		case <- timer.C:
			if _, isLeader := rf.GetState(); isLeader {
				go rf.replicateLogs()
				timer.Reset(AppendEntryInterval) // timer has expired, so don't need call stop()
			} else {
				return 								// not a leader anymore, stop send appendEntry RPC
			}
		case <-rf.shutdown:
			return
		}
	}
}

//
// new leader call this, reinitialize index array
//
func (rf *Raft) initIndex() {
	length := len(rf.peers)
	rf.nextIndex = make([]int, length)
	rf.matchIndex = make([]int, length)
	for i := 0; i < length; i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}

//
// send heartbeat or replicate new logs
//
func (rf *Raft) replicateLogs() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i)
	}
}

//
// if send heartbeat, the entry would be empty and rf.nextIndex[server] = rf.lastLogIndex + 1
//
func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	DPrintf("(info) (%d) send LogEntry[%d - %d] to (%d) at term[%d]", rf.me, rf.nextIndex[server], rf.lastLogIndex, server, rf.currentTerm)
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		rf.nextIndex[server] - 1,
		rf.logs[rf.nextIndex[server]-1].LogTerm,
		[]LogEntry{},
		rf.commitIndex,
	}
	for j := rf.nextIndex[server]; j <= rf.lastLogIndex; j++ {
		args.Entries = append(args.Entries, rf.logs[j])
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		prevLogInd := args.PrevLogIndex
		entryLen := len(args.Entries)
		if prevLogInd + entryLen >= rf.nextIndex[server] { 	// in case that the reply come out of order
			rf.nextIndex[server] = prevLogInd + entryLen + 1
			rf.matchIndex[server] = prevLogInd + entryLen
		}
		if rf.canCommit(prevLogInd + entryLen) {			// check whether the logs have existed in a majority of servers
			DPrintf("(info) (%d) commit index from [%d] to [%d]", rf.me, rf.commitIndex, prevLogInd+entryLen)
			rf.commitIndex = prevLogInd + entryLen
			rf.persist()
			rf.applyNotify <- struct{}{}
		}
	} else {
		if reply.Term > rf.currentTerm { 					// the leader is out of date
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.resetElectionTimer(randTimeoutDuration())
			return
		}
		// if leaderTerm != currentTerm, then this is an old reply for the RPC sent from this server's old term
		// which means this server was a leader at an early term, sent the RPC and failed
		if reply.LeaderTerm == rf.currentTerm && rf.state == LEADER {	// fails because of log inconsistency
			rf.nextIndex[server] = Max(1, reply.HintIndex)
		}
	}
}

//
// notify the client to apply the logs to state machine
//
func (rf *Raft) applyToService() {
	for {
		select {
		case <-rf.applyNotify: 	// to ensure that applyToClient won't get out of order
			DPrintf("(info) (%d) ---- CALL [applyToService] ---- commitIndex [%d]", rf.me, rf.commitIndex)
			var msg []ApplyMsg
			rf.mu.Lock() 		// to ensure that the subsequent applyToService's applyMsg does not overlap with the current one
			for rf.lastApplied < rf.commitIndex {
				ind := rf.lastApplied + 1
				applyMsg := ApplyMsg{
					true,
					rf.logs[ind].Command,
					rf.logs[ind].LogIndex,
					rf.logs[ind].LogTerm,
				}
				msg = append(msg, applyMsg)
				rf.lastApplied += 1
			}
			rf.mu.Unlock()

			for _, m := range msg {
				rf.applyCh <- m
				DPrintf("(info) (%d) apply [%d] to service", rf.me, m.CommandIndex)
			}
		case <-rf.shutdown:
			return
		}
	}
}

//
// whether an index can be committed or not
//
func (rf *Raft) canCommit(index int) bool {
	result := false
	// the new leader cannot commit logs from old term in terms of counting the number of replicas
	if index <= rf.lastLogIndex && index > rf.commitIndex && rf.logs[index].LogTerm == rf.currentTerm {
		count := 0
		majorityNum := len(rf.peers) / 2 + 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index {
				count++
			}
		}
		if count >= majorityNum {
			result = true
		}
	}
	return result
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	success := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {							// the RPC was from a former leader
		success = false
	} else {
		rf.resetElectionTimer(randTimeoutDuration()) 		// receive valid RPC from leader
		rf.state = FOLLOWER									// candidate become follower when a valid leader send AppendRPC
		rf.currentLeader = args.LeaderId
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		if rf.lastLogIndex < args.PrevLogIndex {
			success = false									// doesn't have an entry match prevLogIndex
			reply.HintIndex = rf.lastLogIndex + 1
		} else {
			if rf.logs[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
				success = false 							// conflicts, same index but different terms
				curTerm := rf.logs[args.PrevLogIndex].LogTerm
				curInd := args.PrevLogIndex
				for rf.logs[curInd-1].LogTerm == curTerm && curInd-1 > rf.commitIndex {	// find the first index in current conflicting term
					curInd -= 1
				}
				reply.HintIndex = curInd
			} else {
				prevLogIndex := args.PrevLogIndex
				i := 0
				for ; i < len(args.Entries); i++ {
					if prevLogIndex+1+i > rf.lastLogIndex {
						break
					}
					if rf.logs[prevLogIndex + 1 + i].LogTerm != args.Entries[i].LogTerm {
						rf.lastLogIndex = prevLogIndex + i
						truncationEndIndex := rf.lastLogIndex+1
						rf.logs = append(rf.logs[:truncationEndIndex]) // delete any conflicting log entries
						break
					}
				}
				for ; i < len(args.Entries); i++ {
					rf.logs = append(rf.logs, args.Entries[i])
					rf.lastLogIndex += 1
				}
				DPrintf("(info) (%d) append log from [%d] to [%d]", rf.me, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.lastLogIndex)))
					rf.applyNotify <- struct{}{}			// notify the routine to apply log to client
				}
			}
		}
	}
	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = success
	reply.LeaderTerm = args.Term
	DPrintf("(info) (%d) reply success [%t]", rf.me, success)
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

	if _, isLeader = rf.GetState(); isLeader {
		rf.mu.Lock()
		index = rf.lastLogIndex + 1
		term = rf.currentTerm
		entry := LogEntry{
			index,
			term,
			command,
		}
		if index < len(rf.logs) {
			rf.logs[index] = entry
		} else {
			rf.logs = append(rf.logs, entry)
		}
		rf.matchIndex[rf.me] = index
		rf.lastLogIndex++
		rf.persist()
		DPrintf("(info) (%d) client send an entry: index[%d], term[%d]", rf.me, index, term)
		go rf.replicateLogs()
		rf.mu.Unlock()
	}

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
	rf.applyNotify = make(chan struct{}, 64)
	rf.logs = []LogEntry{
		{0,0,nil},
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.applyCh = applyCh

	DPrintf("(info) (%d) initiate a raft server", me)

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

	go rf.applyToService()

	return rf
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {
	// Always stop a electionTimer before reusing it. See https://golang.org/pkg/time/#Timer.Reset
	// We ignore the value return from Stop() because if Stop() return false, the value inside the channel has been drained out
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}
