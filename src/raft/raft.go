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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg To the service (or
// tester) on the same server, via the applyCh passed To Make(). set
// CommandValid To true To indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want To send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid To false for these
// other uses.
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

const Follower_Prepare_Time = 200 * time.Millisecond
const Min_Election_Time = 500 * time.Millisecond
const Max_Election_Time = 1200 * time.Millisecond
const Loop_Wait_Time = 50 * time.Millisecond
const Heartbeat_Time = 50 * time.Millisecond
const Request_Time = 5 * time.Millisecond

const (
	BeginRaft = iota
	PrepareElection
	InElection
	//ElectionOK
	ElectionFailed
	ElectionTimeout
	ElectionResultSynced
)

// A Go object implementing a single Raft peer.
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
	state             int       // state: Follower/Candidate/Leader
	lastHeartbeatTime time.Time // the last time received heartbeat

	electionStatus    int32
	electionStartTime time.Time
	electionEndTime   time.Time

	// vote status
	votesFromOthers   map[int]bool
	voteToOthers      map[int]int
	electionCompleted bool
	leaderTerm        int
}

func (rf *Raft) getElectionStatus() int32 {
	return atomic.LoadInt32(&rf.electionStatus)
}

func (rf *Raft) setElectionStatus(val int32) {
	atomic.StoreInt32(&rf.electionStatus, val)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	isleader = rf.state == Leader
	term = rf.curTerm

	return term, isleader
}

// save Raft's persistent state To stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants To switch To snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From int
	To   int
	Term int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	From    int
	To      int
	Term    int
	Success bool
}

const (
	ElectionCompleted = iota
	Heartbeat
	AppendEntries
)

type AppendEntriesRequest struct {
	Type     int
	Term     int
	LeaderId int
	To       int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	_, exist := rf.voteToOthers[args.Term]
	if (rf.curTerm <= args.Term) && (!exist) {
		reply.From = args.To
		reply.To = args.From
		reply.Term = rf.curTerm
		reply.Success = true

		rf.voteToOthers[args.Term] = args.From
	} else {
		reply.From = args.To
		reply.To = args.From
		reply.Term = rf.curTerm
		reply.Success = false
	}

	LogLn(rf.me, "RequestVote: it's term ", rf.curTerm, ", i just received request vote of term", args.Term, " from ", args.From, ", and i finally reply ", reply.Success)
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	LogLn(rf.me, "AppendEntries: cur term - ", rf.curTerm, ", leader term - ", args.Term)

	if rf.curTerm <= args.Term && rf.me != args.LeaderId {
		if args.Type == ElectionCompleted {
			rf.electionCompleted = true
			rf.leaderTerm = args.Term

			LogLn(rf.me, "AppendEntries: ElectionCompleted: cur term - ", rf.curTerm, ", leader term - ", args.Term)
		}

		rf.lastHeartbeatTime = time.Now()

		reply.Term = args.Term
		reply.Success = true

	} else if rf.curTerm > args.Term {
		reply.Term = rf.curTerm
		reply.Success = false
	}
}

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
func (rf *Raft) sendRequestVote(ch chan *RequestVoteReply, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	go rf.sendRequestVoteSync(ch, server, args, reply)
}

func (rf *Raft) sendRequestVoteSync(ch chan *RequestVoteReply, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		reply.Success = false
	}

	ch <- reply
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) {
	go rf.sendAppendEntriesSync(server, args, reply)
}

func (rf *Raft) sendAppendEntriesSync(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() To
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests To fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() To check whether it should electionCompleted.
func (rf *Raft) Kill() {
	LogLn(rf.me, "killed...")
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getSuccReplyCount() int {
	count := 0

	for _, r := range rf.votesFromOthers {
		if r {
			count++
		}
	}

	return count
}

func (rf *Raft) getTotalReplyCount() int {
	return len(rf.votesFromOthers)
}

func (rf *Raft) alreadyReplied(from int) bool {
	_, exist := rf.votesFromOthers[from]
	return exist
}

func (rf *Raft) requestVotes() (ret bool) {
	rf.mu.Lock()

	ch := make(chan *RequestVoteReply)

	/*send async request*/
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		LogLn(rf.me, "requestVotes: send async req to ", i)

		req := RequestVoteArgs{rf.me, i, rf.curTerm}
		reply := RequestVoteReply{0, 0, rf.curTerm, false}

		rf.sendRequestVote(ch, i, &req, &reply)

	}

	/*check reply*/
	for true {
		select {
		case result := <-ch:
			{
				if rf.alreadyReplied(result.From) {
					break
				}

				if result.Success {
					LogLn(rf.me, "requestVotes: receive success reply of term ", rf.curTerm, " from ", result.From)

					// record votes according To reply
					rf.votesFromOthers[result.From] = true

					if rf.getSuccReplyCount()+1 > len(rf.peers)/2 {
						LogLn(rf.me, "requestVotes: election success")

						// notify election result to peers
						rf.heartbeat(ElectionCompleted)

						rf.setElectionStatus(ElectionResultSynced)

						rf.mu.Unlock()

						return true
					}
				} else {
					rf.votesFromOthers[result.From] = false
				}

				if rf.getTotalReplyCount() == len(rf.peers)-1 {
					LogLn(rf.me, "requestVotes: request failed")
					goto fail
				}
			}

			break

		case <-time.After(Request_Time):
			LogLn(rf.me, "requestVotes: request timeout")
			goto fail
		}
	}

fail:
	rf.setElectionStatus(ElectionFailed)
	rf.mu.Unlock()

	return false
}

func (rf *Raft) initElectionRound() {
	// vote for myself
	rf.votesFromOthers = make(map[int]bool)
	rf.voteToOthers[rf.curTerm] = rf.me
	rf.setElectionStatus(InElection)
	rf.electionStartTime = time.Now()
	rf.electionCompleted = false
}

func (rf *Raft) startNewElection() int64 {
	rf.curTerm++
	rf.initElectionRound()

	rand.Seed(time.Now().UnixNano())
	t := Max_Election_Time - Min_Election_Time
	waitTime := rand.Int63n(t.Nanoseconds()) + Min_Election_Time.Nanoseconds()

	// goroutine: send request
	go rf.requestVotes()

	return waitTime
}

func (rf *Raft) prepareTimeout() (ret bool) {
	//LogLn(rf.me, " prepareTimeout: lastHeartbeatTime ", rf.lastHeartbeatTime.UnixMilli())
	return time.Now().Sub(rf.lastHeartbeatTime) >= Follower_Prepare_Time
}

func (rf *Raft) heartbeatTimeout() (ret bool) {
	return time.Now().Sub(rf.lastHeartbeatTime) >= Heartbeat_Time
}

func (rf *Raft) checkElectionStatus(timeout time.Duration) (ret int32) {
	time.Sleep(time.Duration(Loop_Wait_Time))

	//LogLn(rf.me, "checkElectionStatus: i'm ", rf.me, "now:", time.Now(), ", start:", rf.electionStartTime, " timeout:", timeout)

	if time.Now().Sub(rf.electionStartTime) >= timeout {
		rf.setElectionStatus(ElectionTimeout)
		return rf.getElectionStatus()
	}

	return rf.getElectionStatus()
}

//
//func (rf *Raft) notifyElectionResult() bool {
//	count := 0
//
//	for i, _ := range rf.peers {
//
//		if i == rf.me {
//			continue
//		}
//
//		args := AppendEntriesRequest{ElectionCompleted, rf.curTerm, rf.me, i}
//		reply := AppendEntriesReply{}
//		rv := rf.sendAppendEntries(i, &args, &reply)
//		if rv && reply.Success {
//			count++
//			LogLn(rf.me, "notifyElectionResult: count:", count)
//		}
//	}
//
//	LogLn(rf.me, "notifyElectionResult: count+1 ", count+1, ", peers majority ", len(rf.peers)/2)
//	if count+1 > len(rf.peers)/2 {
//		LogLn(rf.me, "notifyElectionResult: ok")
//		return true
//	}
//
//	return false
//}

func (rf *Raft) heartbeat(hbType int) {
	for i, _ := range rf.peers {

		if i == rf.me {
			continue
		}

		args := AppendEntriesRequest{hbType, rf.curTerm, rf.me, i}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i, &args, &reply)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var waitTime int64

	for rf.killed() == false {

		// Your code here To check if a leader election should
		// be started and To randomize sleeping time using
		// time.Sleep().

		// prepare
		if rf.state == Follower {
			if rf.prepareTimeout() {

				// transfer To Candidate
				rf.state = Candidate
				rf.waitOlderReqThreadExit()
				waitTime = rf.startNewElection()

				LogLn(rf.me, "ticker: prepare ok. timeout:", waitTime)
			}
		}

		if rf.state == Candidate {
			status := rf.checkElectionStatus(time.Duration(waitTime))

			if rf.electionCompleted {
				rf.state = Follower
				rf.curTerm = rf.leaderTerm
				rf.electionCompleted = false
				rf.setElectionStatus(ElectionResultSynced)

				LogLn(rf.me, " election completed, transfer to Follower, lastHeartbeatTime:", rf.lastHeartbeatTime.UnixMilli())
			} else {
				// start a new election once the older election is timeout
				if status == ElectionTimeout {
					rf.waitOlderReqThreadExit()
					waitTime = rf.startNewElection()

					LogLn(rf.me, "ticker: election timeout. try to redo election...waittime:", waitTime)
				} else if status == ElectionResultSynced {
					rf.state = Leader
					rf.lastHeartbeatTime = time.Now()
					LogLn(rf.me, " ElectionResultSynced ok, transfer to Leader, lastHeartbeatTime:", rf.lastHeartbeatTime.UnixMilli())
				}
			}
		}

		if rf.state == Leader {
			// heartbeat
			if rf.heartbeatTimeout() {
				rf.heartbeat(Heartbeat)
				rf.lastHeartbeatTime = time.Now()
			}
		}
	}
}

func (rf *Raft) initRaft() {
	rf.state = Follower
	rf.curTerm = 0
	rf.lastHeartbeatTime = time.Unix(0, 0)
	rf.setElectionStatus(PrepareElection)
	rf.electionStartTime = time.Unix(0, 0)
	rf.electionEndTime = time.Unix(0, 0)
	rf.electionCompleted = false
	rf.voteToOthers = make(map[int]int)
}

func (rf *Raft) waitOlderReqThreadExit() {
	rf.mu.Lock()
	rf.mu.Unlock()
}

// the service or tester wants To create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server To
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft To send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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
