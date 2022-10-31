package raft

//
// this is an outline of the API that raft must expose To
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed To the log, each Raft peer
//   should send an ApplyMsg To the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg To the service (or
// tester) on the same server, via the applyCh passed To Make(). set
// CommandValid To true To indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want To send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid To false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

const Follower_Prepare_Time = 2 * time.Second
const Min_Election_Time = 3 * time.Second
const Max_Election_Time = 5 * time.Second

const (
	BeginRaft = iota
	PrepareElection
	InElection
	ElectionOK
	ElectionTimeout
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock To protect shared access To this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object To hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	curTerm           int
	state             int       // state: Follower/...
	lastHeartbeatTime time.Time // the last time received heartbeat

	electionStatus    int
	electionStartTime time.Time
	electionEndTime   time.Time
	electionOK        int32

	// vote status
	votesFromOthers map[int]bool
	voteToOther     bool
}

func (rf *Raft) getElectionSuccess() bool {
	return atomic.LoadInt32(&rf.electionOK) > 0
}

func (rf *Raft) setElectionSuccess(val bool) {
	var ival int32
	if val {
		ival = 1
	} else {
		ival = 0
	}

	atomic.StoreInt32(&rf.electionOK, ival)
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
// save Raft's persistent state To stable storage,
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
// A service wants To switch To snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up To and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From int
	To   int
	Term int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	From    int
	To      int
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.curTerm == args.Term {
		reply.From = args.To
		reply.To = args.From
		reply.Term = args.Term
		reply.Success = true
	} else {
		reply.From = args.To
		reply.To = args.From
		reply.Term = rf.curTerm
		reply.Success = false
	}
}

//
// example code To send a RequestVote RPC To a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed To Call() must be
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
// Call() is guaranteed To return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need To implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC To work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants To start
// agreement on the next command To be appended To Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed To the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
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
// but it does call the Kill() method. your code can use killed() To
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests To fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() To check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) requestVotes() (ret bool) {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		req := RequestVoteArgs{rf.me, i, rf.curTerm}
		reply := RequestVoteReply{0, 0, rf.curTerm, false}
		if rf.sendRequestVote(i, &req, &reply) && reply.Success {
			// stat votes according To reply
			rf.votesFromOthers[reply.From] = true

			if len(rf.votesFromOthers) > len(rf.peers)/2 {
				// election ok
				rf.setElectionSuccess(true)
			}
		} else {
			fmt.Printf("request vote To %d failed. \n", i)
		}
	}

	return true
}

func (rf *Raft) startNewElection() int64 {
	rf.curTerm++

	rf.electionStatus = InElection
	rf.electionStartTime = time.Now()

	rand.Seed(time.Now().UnixNano())
	t := Max_Election_Time - Min_Election_Time
	waitTime := rand.Int63n(t.Nanoseconds()) + Min_Election_Time.Nanoseconds()

	// goroutine: send request
	go rf.requestVotes()

	return waitTime
}

func (rf *Raft) prepareTimeout() (ret bool) {
	return time.Now().Sub(rf.lastHeartbeatTime) >= Follower_Prepare_Time
}

func (rf *Raft) waitForElectionFinish(start time.Time, timeout time.Duration) (ret int) {
	if time.Now().Sub(start) >= timeout {
		return ElectionTimeout
	}

	if rf.getElectionSuccess() {
		rf.electionStatus = ElectionOK
		return ElectionOK
	}

	time.Sleep(time.Duration(time.Millisecond * 10))
	return rf.electionStatus
}

func (rf *Raft) onElectionTimeout() {
	rf.electionStatus = ElectionTimeout
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here To check if a leader election should
		// be started and To randomize sleeping time using
		// time.Sleep().

		var waitTime int64

		// prepare
		if rf.state == Follower {
			if rf.prepareTimeout() {
				// transfer To Candidate
				rf.state = Candidate
				waitTime = rf.startNewElection()
			}
		}

		// rf.state
		if (rf.state == Candidate) && (rf.electionStatus == ElectionTimeout) {
			waitTime = rf.startNewElection()
		}

		if rf.electionStatus == InElection {
			status := rf.waitForElectionFinish(rf.electionStartTime, time.Duration(waitTime))
			if status == ElectionTimeout {
				// todo: election timeout or election Success
				rf.onElectionTimeout()
			}
		}
	}
}

func (rf *Raft) initRaft() {
	rf.state = Follower
	rf.curTerm = 0
	rf.lastHeartbeatTime = time.Unix(0, 0)
	rf.electionStatus = BeginRaft
	rf.electionStartTime = time.Unix(0, 0)
	rf.electionEndTime = time.Unix(0, 0)
}

//
// the service or tester wants To create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server To
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft To send ApplyMsg messages.
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
	rf.initRaft()

	// initialize From state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine To start elections
	go rf.ticker()

	return rf
}
