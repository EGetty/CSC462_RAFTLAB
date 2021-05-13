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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "math/big"
import crand "crypto/rand"
import "log"
import "fmt"
import "bytes"
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
/*func (rf *Raft) CompactLog(lastLogIndex int) {
	rf.Lock()
	defer rf.Unlock()

	if lastLogIndex > rf.commitIndex {
		RaftInfo("Failed to compact log as log index: %d is larger than commit index: %d", rf, lastLogIndex, rf.commitIndex)
	}

	if i, isPresent := rf.findLogIndex(lastLogIndex); isPresent {
		entry := rf.log[i]
		rf.lastSnapshotIndex = entry.Index
		rf.lastSnapshotTerm = entry.Term

		RaftInfo("Compacting log. Removing %d log entries. LastSnapshotEntry(Index: %d, Term: %d)", rf, i+1, entry.Index, entery.Term)
		rf.log = rf.log[i+1]
	}

	rf.persist()
}*/

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	UseSnapshot bool
	Snapshot []byte
}

type LogEntry struct {
	LogIndex int
	LogTerm	 int
	Command	 interface{}
}

type AppendEntriesArgs struct {
	Term		int
	LeaderId	int
	PrevLogIndex	int
	PrevLogTerm	int
	CommitIndex	int
	Len		int
	Entries		[]LogEntry
}

type AppendEntriesReply struct {
	Term		int
	ConflictIndex	int
	Success		bool
}

type InstallSnapshotArgs struct {
	Term			int
	LastIncludedTerm	int
	LeaderId		int
	LastIncludedIndex	int
	Data			[]byte
}

type InstallSnapshotReply struct {
	Err	Err
	Term	int
}

type Err string
const(
	OK = "OK"
	ErrRPCFail = "ErrRPCFail"
)
type serverState int32
const ElectionTimeout = time.Duration(1000 * time.Millisecond)
const AppendEntriesInterval = time.Duration(100 * time.Millisecond)
const(
	Leader serverState = iota
	Follower
	Candidate
)

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func init() {
	labgob.Register(LogEntry{})
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	seed := bigx.Int64()
	rand.Seed(seed)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

//
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state		serverState//current state of server
	isleader	bool	  //set to true when server becomes leader, false otherwise
	leaderId	int
	currentTerm	int	  //latest term server has seen
	votedFor	int	  //candidateId that received vote in current term
	log		[]LogEntry//log entries
	logIndex	int	  //index of next log entry
	commitIndex	int	  //index of highest log entry known to be committed
	lastIncludedIndex int
	lastApplied	int	  //index of highest log entry applied to state machine
	nextIndex	[]int	  //for each server, index of the next log entry to send to that server
	matchIndex	[]int	  //for each server, index of highest log entry known to be replicated on server
	applyCh		chan ApplyMsg
	notifyApplyCh	chan struct{}
	electionTimer	*time.Timer//new election on timeout
	shutdown	chan struct{}

}

func (state serverState) String() string {
	switch state {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	default:
		return "Candidate"
	}
}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for follower := 0; follower < len(rf.peers); follower++ {
		if follower != rf.me {
			go rf.sendLogEntry(follower)
		}
	}
}

func newRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return time.Duration(minDuration + extra)
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}

func (rf *Raft) getOffsetIndex(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) getRangeEntry(fromInclusive, toExclusive int) []LogEntry {
	from := rf.getOffsetIndex(fromInclusive)
	to := rf.getOffsetIndex(toExclusive)
	return append([]LogEntry{}, rf.log[from:to]...)
}

func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.getEntry(index).LogTerm == rf.currentTerm {
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= index {
				count += 1
			}
		}
		return count >= majority
	} else {
		return false
	}
}

func (rf *Raft) notifyNewLeader() {
	rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "NewLeader"}
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor, rf.leaderId = -1, -1
	rf.persist()
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
}

func (rf *Raft) sendSnapshot(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastIncludedIndex, LastIncludedTerm: rf.getEntry(rf.lastIncludedIndex).LogTerm, Data: rf.persister.ReadSnapshot()}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.peers[follower].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		} else {
			rf.nextIndex[follower] = Max(rf.nextIndex[follower], rf.lastIncludedIndex+1)
			rf.matchIndex[follower] = Max(rf.matchIndex[follower], rf.lastIncludedIndex)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendLogEntry(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[follower] <= rf.lastIncludedIndex {
		go rf.sendSnapshot(follower)
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1
	prevLogTerm := rf.getEntry(prevLogIndex).LogTerm
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: 0, Entries: nil}
	if rf.nextIndex[follower] < rf.logIndex {
		entries := rf.getRangeEntry(prevLogIndex+1, rf.logIndex)
		args.Entries, args.Len = entries, len(entries)
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	if rf.peers[follower].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term) //leader now obsolete
			} else { //inconsistency with follower
				rf.nextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.logIndex))
				if rf.nextIndex[follower] <= rf.lastIncludedIndex {
					go rf.sendSnapshot(follower)
				}
			}
		} else {
			prevLogIndex, logEntriesLen := args.PrevLogIndex, args.Len
			if prevLogIndex+logEntriesLen >= rf.nextIndex[follower] {
				rf.nextIndex[follower] = prevLogIndex + logEntriesLen + 1
				rf.matchIndex[follower] = prevLogIndex + logEntriesLen
			}
			toCommitIndex := prevLogIndex + logEntriesLen
			if rf.canCommit(toCommitIndex) {
				rf.commitIndex = toCommitIndex
				rf.persist()
				rf.notifyApplyCh <- struct{}{}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) apply() {
	for {
		select {
		case <- rf.notifyApplyCh:
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry
			if rf.lastApplied < rf.lastIncludedIndex {
				commandValid = false
				rf.lastApplied = rf.lastIncludedIndex
				entries = []LogEntry{{LogIndex: rf.lastIncludedIndex, LogTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}}
			} else if rf.lastApplied < rf.logIndex && rf.lastApplied < rf.commitIndex {
				commandValid = true
				entries = rf.getRangeEntry(rf.lastApplied+1, rf.commitIndex+1)
				rf.lastApplied = rf.commitIndex
			}
			rf.persist()
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entry.LogIndex, CommandTerm: entry.LogTerm, Command: entry.Command}
			}
		case <-rf.shutdown:
			return
		}
	}
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
	isleader = rf.state == Leader
	//println("Getstate (server:", rf.me, ") (term:", term, ") (role:", rf.state, ")")
	return term, isleader
}

func (rf *Raft) getEntry(i int) LogEntry {
	offsetIndex := rf.getOffsetIndex(i)
	return rf.log[offsetIndex]
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	reply.Term = args.Term
	rf.leaderId = args.LeaderId
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower
	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	if prevLogIndex < rf.lastIncludedIndex {
		reply.Success, reply.ConflictIndex = false, rf.lastIncludedIndex+1
		return
	}
	if logIndex <= prevLogIndex || rf.getEntry(prevLogIndex).LogTerm != args.PrevLogTerm {
		conflictIndex := Min(rf.logIndex-1, prevLogIndex)
		conflictTerm := rf.getEntry(conflictIndex).LogTerm
		floor := Max(rf.lastIncludedIndex, rf.commitIndex)
		for ; conflictIndex > floor && rf.getEntry(conflictIndex - 1).LogTerm == conflictTerm; conflictIndex-- {
		}
		reply.Success, reply.ConflictIndex = false, conflictIndex
		return
	}
	reply.Success, reply.ConflictIndex = true, -1
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex {
			break
		}
		if rf.getEntry(prevLogIndex + 1 + i).LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = prevLogIndex + 1 + i
			truncationEndIndex := rf.getOffsetIndex(rf.logIndex)
			rf.log = append(rf.log[:truncationEndIndex])
			break
		}
	}
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+args.Len))
	rf.persist()
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	if rf.commitIndex > oldCommitIndex {
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err = OK
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.leaderId = args.LeaderId
	if args.LastIncludedIndex > rf.lastIncludedIndex {
		truncationStartIndex := rf.getOffsetIndex(args.LastIncludedIndex)
		rf.lastIncludedIndex = args.LastIncludedIndex
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = Max(rf.logIndex, rf.lastIncludedIndex)
		rf.logIndex = Max(rf.logIndex, rf.lastIncludedIndex+1)
		if truncationStartIndex < len(rf.log) {
			rf.log = append(rf.log[truncationStartIndex:])
		} else {
			rf.log = []LogEntry{{args.LastIncludedIndex, args.LastIncludedTerm, nil}}
		}
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		if rf.commitIndex > oldCommitIndex {
			rf.notifyApplyCh <- struct{}{}
		}
	}
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	rf.persist()
}

func (rf *Raft) tick() {
	timer := time.NewTimer(AppendEntriesInterval)
	for {
		select {
		case <- rf.shutdown:
			return
		case <- timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go rf.replicate()
			timer.Reset(AppendEntriesInterval)
		}
	}
}

func (rf *Raft) solicit(server int, args RequestVoteArgs, replyCh chan<- RequestVoteReply) {
	var reply RequestVoteReply
	if !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
		reply.Err, reply.Server = ErrRPCFail, server
	}
	replyCh <- reply
}

func (rf *Raft) campaign() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.leaderId = -1
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	currentTerm, lastLogIndex, me := rf.currentTerm, rf.logIndex-1, rf.me
	lastLogTerm := rf.getEntry(lastLogIndex).LogTerm
	//fmt.Printf("node %d at term %d start election\n", rf.me, rf.currentTerm)
	rf.persist()
	args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	electionDuration := newRandDuration(ElectionTimeout)
	rf.resetElectionTimer(electionDuration)
	timer := time.After(electionDuration)
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != me {
			//fmt.Printf("Requesting vote...\n")
			go rf.solicit(i, args, replyCh)
		}
	}
	voteCount, threshold := 0, len(rf.peers)/2
	for voteCount < threshold {
		select {
		case <- rf.shutdown:
			return
		case <- timer:
			//fmt.Printf("Election timeout\n")
			return
		case reply := <-replyCh:
			if reply.Err != OK {
				go rf.solicit(reply.Server, args, replyCh)
			} else if reply.VoteGranted {
				voteCount += 1
			} else {
				//fmt.Printf("No votes\n")
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.stepDown(reply.Term)
				}
				rf.mu.Unlock()
			}
		}
	}
	//fmt.Printf("Total votes %d\n", voteCount)
	rf.mu.Lock()
	if rf.state == Candidate {
		fmt.Printf("Candidate %d becoming leader\n", rf.me)
		rf.state = Leader
		rf.initIndex()
		go rf.tick()
		go rf.notifyNewLeader()
	}
	rf.mu.Unlock()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
	    d.Decode(&rf.log) != nil {
		log.Fatal("Error in unmarshall raft state")
	//} else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	}
	print("\nReading persist: ")
	print("\nCurrent Term: ", currentTerm)
	print("\nVoted For: ", votedFor)
	print("\nLast Included Index: ", lastIncludedIndex)
	print("\nLog Index: ", logIndex)
	print("\nLast Applied: ", lastApplied, "\n")
	rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.logIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		int //current term of candidate
	CandidateId	int //ID of candidate requesting vote
	LastLogIndex	int //index of candidate's last log entry
	LastLogTerm	int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Err		Err
	Server		int
	VoteGranted	bool
	Term		int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err, reply.Server = OK, rf.me
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		if rf.state != Follower {
			rf.resetElectionTimer(newRandDuration(ElectionTimeout))
			rf.state = Follower
		}
	}
	rf.leaderId = -1
	reply.Term = args.Term
	lastLogIndex := rf.logIndex-1
	lastLogTerm := rf.getEntry(lastLogIndex).LogTerm
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	index = rf.logIndex
	entry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}
	if offsetIndex := rf.getOffsetIndex(rf.logIndex); offsetIndex < len(rf.log) {
		rf.log[offsetIndex] = entry
	} else {
		rf.log = append(rf.log, entry)
	}
	term = rf.currentTerm
	rf.matchIndex[rf.me] = rf.logIndex
	rf.logIndex += 1
	rf.persist()
	go rf.replicate()
	//println("Index: ", index, " Term: ", term, "isLeader: ", isLeader)
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
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.logIndex = 1
	rf.log = []LogEntry{{0, 0, nil}}
	rf.state = Follower
	rf.commitIndex = 0
	rf.shutdown = make(chan struct{})
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.electionTimer = time.NewTimer(newRandDuration(ElectionTimeout))
	go rf.apply()
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.campaign()
			case <-rf.shutdown:
				return
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//print("Encoded persister raft state: ", persister.ReadRaftState(),"\n")

	return rf
}
