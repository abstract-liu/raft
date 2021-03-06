package raft
// todo: add mutex to protect essential part


//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new raftLog entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the raftLog, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive raftLog entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed raftLog entry.
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


const(
	Leader = iota
	Follower
	Candidate
)

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

	//election state
	lastHeartTime time.Time
	role int

	//persistent state on all servers
	currentTerm int
	votedFor    int
	raftLog     []Log

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex []int
	matchIndex []int

	applyCh chan ApplyMsg
}

type Log struct {
	RaftLogTerm int
	Command     interface{}
	Index       int
}

/*
func (log *Log) String() string {
	return "RaftLogTerm:" + strconv.Itoa(log.RaftLogTerm)
}
 */

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Printf("serialize currentTerm failed!")
		return
	}

	err = e.Encode(rf.votedFor)
	if err != nil {
		log.Printf("serialize votedFor failed!")
		return
	}

	err = e.Encode(rf.raftLog)
	if err != nil {
		log.Printf("serialize raftLog failed!")
		return
	}

	rf.persister.SaveRaftState(w.Bytes())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var raftLog []Log

	err := d.Decode(&currentTerm)
	if err != nil {
		log.Printf("decoding currentTerm failed!")
		return
	} else {
		rf.currentTerm = currentTerm
	}

	err = d.Decode(&votedFor)
	if err != nil {
		log.Printf("decoding votedFor failed!")
		return
	} else {
		rf.votedFor = votedFor
	}

	err = d.Decode(&raftLog)
	if err != nil {
		log.Printf("decoding raftLog failed!")
		return
	} else {
		rf.raftLog = raftLog
		//log.Printf("server %d decoding raftlog %v", rf.me, rf.raftLog)
	}

}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//raftLog.Printf("server %d receive vote request from %+v, and server term %d voteFor %d", rf.me, args, rf.currentTerm, rf.votedFor)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.transfer2Follower(args.Term, "requestVote")
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isYoungerThanCandidate(args) {
			rf.votedFor = args.CandidateId
			go rf.persist()
			reply.VoteGranted = true
			//log.Printf("server %d grant vote to %d, server term:%d, index:%d, follower term:%d, index:%d", rf.me, rf.votedFor, args.Term, args.LastLogIndex, rf.raftLog[len(rf.raftLog)-1].RaftLogTerm, rf.raftLog[len(rf.raftLog)-1].Index)

		}
	}
}

func (rf *Raft) isYoungerThanCandidate(args *RequestVoteArgs) bool {
	rfLastLogTerm := rf.raftLog[len(rf.raftLog)-1].RaftLogTerm
	if args.LastLogTerm < rfLastLogTerm{
		return false
	} else if args.LastLogTerm == rfLastLogTerm {
		if args.LastLogIndex < len(rf.raftLog)-1 {
			return false
		}
	}
	return true


	/*
		if args.LastLogIndex < len(rf.raftLog)-1 {
			return false
		} else if args.LastLogIndex == len(rf.raftLog)-1 {
			if args.LastLogTerm < rf.raftLog[len(rf.raftLog)-1].RaftLogTerm {
				return false
			}
		}
		return true
	*/
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
// agreement on the next command to be appended to Raft's raftLog. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft raftLog, since the leader
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
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (2B).

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	newLog := Log{
		RaftLogTerm: rf.currentTerm,
		Command:     command,
		Index:       len(rf.raftLog),
	}
	rf.raftLog = append(rf.raftLog, newLog)
	index = len(rf.raftLog)-1
	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()

	go rf.persist()

	//log.Printf("start function server receive command %v, index is %d", newLog, index)

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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.raftLog = make([]Log, 1)
	rf.raftLog[0] = Log{ RaftLogTerm: 0}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.feelHeart()
	go rf.applyCommit()

	return rf
}

func (rf *Raft) startVote(){
	voteNum := 1
	//log.Printf("server %d pass election time, and start voting now", rf.me)
	for peer := range rf.peers{
		go func(server int){
			if server == rf.me {
				return
			}

			lastLogIndex := len(rf.raftLog) - 1
			lastLogTerm := rf.raftLog[lastLogIndex].RaftLogTerm
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogTerm: lastLogTerm, LastLogIndex: lastLogIndex}
			reply := RequestVoteReply{}
			resp := rf.sendRequestVote(server, &args, &reply)

			if !resp {
				return
			}

			if reply.VoteGranted {
				//log.Printf("server %d receive a vote from server %d, currentTerm: %d", rf.me, server, rf.currentTerm)
				voteNum += 1
				if voteNum > len(rf.peers)/2 && rf.role == Candidate {
					rf.mu.Lock()
					rf.role = Leader
					log.Printf("server %d successive become a leader", rf.me)
					rf.startLeaderControl()
					rf.leaderApply()
					rf.mu.Unlock()
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.transfer2Follower(reply.Term, "vote reply")
					return
				}
			}
		}(peer)
	}
}

func (rf *Raft) feelHeart() {
	for {
		electionTime := rand.Intn(500) + 300
		nowTime := time.Now()
		time.Sleep(time.Duration(electionTime) * time.Millisecond)

		rf.mu.Lock()
		if  rf.lastHeartTime.Before(nowTime) && rf.role != Leader{
			rf.role = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			go rf.persist()
			rf.lastHeartTime = time.Now()

			go rf.startVote()
		}
		rf.mu.Unlock()
	}
}



type EntityReply struct {
	ReplyTerm int
	Success   bool
}

type EntityArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entities []Log
	LeaderCommit int
}

/*
func (entityArgs *EntityArgs) String() string {
	var log string
	for idx := range entityArgs.Entities{
		log += entityArgs.Entities[idx].String() + ", "
	}
	return "LeaderCommit:" + strconv.Itoa(entityArgs.LeaderCommit) +
		" term:" + strconv.Itoa(entityArgs.Term) +
		", prevLogIndex:" + strconv.Itoa(entityArgs.PrevLogIndex) +
		", prevLogTerm:" + strconv.Itoa(entityArgs.PrevLogTerm) +
		", logEntities: " + log
}
*/


func (rf *Raft) startLeaderControl(){
	//raftLog.Printf("server %d start sending heartbeat package", rf.me)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	logLength := len(rf.raftLog)
	for i := range rf.peers {
		rf.nextIndex[i] = logLength
		rf.matchIndex[i] = -1
	}
	//log.Printf("log length:%d", logLength)

	for peer := range rf.peers {
		go func(server int) {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			for {
				//we suppose send all missing data at once now for simplicity, fault is that in-flight data will be huge
				//this writing is ugly.... but i dont' know better way to write
				rf.mu.Lock()
				if rf.role != Leader  || server == rf.me || currentTerm != rf.currentTerm{
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				go rf.syncLogs(server)
				time.Sleep(100 * time.Millisecond)
			}
		}(peer)
	}

}

func (rf *Raft) syncLogs(server int) {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[server] - 1
	//log.Printf("send to server %d prevLogIndex: %d, len: %d, server: %d", server, prevLogIndex, len(rf.raftLog), rf.me)
	prevLogTerm := rf.raftLog[prevLogIndex].RaftLogTerm
	referSlice := rf.raftLog[prevLogIndex+1:]
	entities := make([]Log, len(referSlice))
	args := EntityArgs{
		Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entities:     entities,
	}
	reply := EntityReply{}
	copy(entities, referSlice)

	rf.mu.Unlock()
	rf.sendRequestEntity(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.ReplyTerm == 0 {
		//log.Printf("server %d fail network", server)
	}else {
		//log.Printf("%+v %+v, from server %d", args, reply, server)
	}

	if reply.Success {
		if prevLogIndex == rf.nextIndex[server] - 1 {
			rf.nextIndex[server] += len(entities)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}

		majorityIndex := rf.getMajorityIndex()
		//raftLog.Printf("majority %d", majorityIndex)
		if majorityIndex > rf.commitIndex && rf.raftLog[majorityIndex].RaftLogTerm == rf.currentTerm {
			rf.commitIndex = majorityIndex
			//raftLog.Printf("commitIndex %d", rf.commitIndex)
		}

	} else {
		if reply.ReplyTerm > rf.currentTerm {
			rf.transfer2Follower(reply.ReplyTerm, "syncLog response")
			return
		}
		//decre nextIndex here
		for prevLogIndex > 0 && rf.raftLog[prevLogIndex].RaftLogTerm == args.PrevLogTerm {
			prevLogIndex -= 1
		}
		rf.nextIndex[server] = prevLogIndex+1
	}
	rf.lastHeartTime = time.Now()
}

func (rf *Raft) RequestEntity(args *EntityArgs, reply *EntityReply) {
	//raftLog.Printf("server %d receive %+v %d", rf.me, args, rf.currentTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartTime = time.Now()
	reply.Success = false
	reply.ReplyTerm = rf.currentTerm



	if args.Term < rf.currentTerm {
		return
	}else if args.Term > rf.currentTerm {
		rf.transfer2Follower(args.Term, "requestEntity")
	}

	if args.PrevLogIndex < len(rf.raftLog) && rf.raftLog[args.PrevLogIndex].RaftLogTerm == args.PrevLogTerm {
		reply.Success = true
		if args.Entities != nil {
			//log.Printf("server %d add raftLog at %d ", rf.me, args.PrevLogIndex+1)
			rf.raftLog = append(rf.raftLog[:args.PrevLogIndex+1], args.Entities...)
			go rf.persist()
		}
		if args.LeaderCommit > rf.commitIndex {
			lastNewIndex := len(rf.raftLog) - 1
			rf.commitIndex = MinInt(args.LeaderCommit, lastNewIndex)
		}

	} else {
		reply.Success = false
	}

}

func (rf *Raft) sendRequestEntity(server int, args *EntityArgs, reply *EntityReply) bool {
	ok := rf.peers[server].Call("Raft.RequestEntity", args, reply)
	return ok
}


func (rf *Raft) getMajorityIndex() int {
	sortSlice := make([]int, len(rf.peers))
	copy(sortSlice, rf.matchIndex)
	sort.Ints(sortSlice)
	//raftLog.Printf("%v", sortSlice)
	return sortSlice[len(rf.peers)/2]
}

func (rf *Raft) transfer2Follower(term int, process string ){
	//log.Printf("server %d find its prevTerm %d smaller than respTerm %d durring %s, change to follwer", rf.me, rf.currentTerm, term, process)
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	go rf.persist()

	rf.lastHeartTime = time.Now()
}

func (rf *Raft) applyCommit(){
	for {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			rf.apply(rf.raftLog[rf.lastApplied])
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) apply(raftLog Log){
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command: raftLog.Command,
		CommandIndex: raftLog.Index,
	}
	if applyMsg.Command == 8888 {
		//log.Printf("server %d apply %+v", rf.me, applyMsg)
	}
	rf.applyCh <- applyMsg
}

func (rf *Raft) leaderApply(){
	applyMsg := ApplyMsg{
		CommandValid: false,
	}
	rf.applyCh <- applyMsg
}