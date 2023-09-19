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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/labrpc"

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type state int

//type ApplyMsg struct {
//	CommandValid bool
//	Command      interface{}
//	CommandIndex int
//}

const (
	Leader    = 1
	Candidate = 2
	Follower  = 3
)

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
	peer_num    int
	voteFor     int
	currentTerm int
	state       state
	vote_num    int
	majority    int
	heartbeat   time.Time
	//log         []map[int]interface{}
	Log_if  Log
	Log_New Log_info
	//random_time int
	ElectionInterval int
	commitIndex      int
	check_point      *sync.Cond
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	check_vote       int
	checkpoint       bool
	chanvote         []chan bool
	wg               *sync.Cond
	applyCh          chan ApplyMsg
	snapshort_index  int
	snapshortflag    bool
	argsch           chan *InstallSnapshotArgs
	waitsnap         []byte
	waitIndex        int
	waitTerm         int
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
	return term, isleader
}
func (rf *Raft) FullGetState() state {
	//var term int
	//var isleader bool
	var server_state state
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//term = rf.currentTerm
	server_state = rf.state
	//isleader = rf.state == Leader
	return server_state
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Fatal("error:", err)
	}
	err = e.Encode(rf.voteFor)
	if err != nil {
		log.Fatalf("error:%v", err)
	}
	err = e.Encode(rf.Log_if.log_index)
	if err != nil {
		log.Fatalf("error:%v", err)
	}
	data := w.Bytes()
	//log.Println("rf.Persistent save Log:", rf.Log_if.log_index)
	rf.persister.Save(data, nil)
}
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	rf.snapshort_index = index
	lastLogTerm := rf.Log_if.entry(index).Term
	//log.Println(rf.Log_if)
	rf.Log_if.Cut(index + 1)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: index,
		LastIncludedTerm:  lastLogTerm,
		Data:              snapshot,
		Done:              true,
	}
	rf.waitsnap = args.Data
	rf.waitIndex = index
	rf.waitTerm = lastLogTerm
	//log.Println("here is snapshot :", args)
	//for peer := 0; peer < len(rf.peers); peer++ {
	//	if peer != rf.me {
	//		reply := &InstallSnapshotReply{}
	//		go func(peer int) {
	//			ok := rf.sendInstallsnapshot(peer, args, reply)
	//			if ok {
	//				if reply.Term == rf.currentTerm {
	//					//rf.matchIndex[peer] = rf.Log_if.last_index()
	//					////rf.matchIndex[peer] = args_AppE.PrevLogIndex + len(rf.Log_if.slice(next))
	//					//rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	//				}
	//			}
	//		}(peer)
	//	}
	//}
	//rf.matchIndex[rf.me] = rf.Log_if.last_index()
	//rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	//rf.snapshortflag = true
	rf.check_point.Broadcast()
	log.Println(rf.me, "send the snapshot ,is Leader?", rf.state == Leader)
	rf.mu.Unlock()
	//rf.argsch <- args

}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.snapshortflag = true
	log.Println(rf.me, "receive the snapshot")
	rf.Log_if.Cut(args.LastIncludedIndex + 1)
	reply.Term = rf.currentTerm
	rf.check_point.Broadcast()
	rf.mu.Unlock()
	rf.argsch <- args
	log.Println("come here")

	return
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	//d.Decode(&rf.currentTerm)
	//d.Decode(&rf.voteFor)
	//d.Decode(&rf.Log_if.log_index)
	var currentTerm int
	var Log []Log_info
	var votefor int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votefor) != nil ||
		d.Decode(&Log) != nil {
		log.Fatalf("read Persistent error")
	} else {
		rf.voteFor = votefor
		rf.currentTerm = currentTerm
		rf.Log_if.log_index = Log
		//log.Println("ReadPersistent Log:", Log)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Cand_Term    int
	Candidate_Id int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Curr_Term   int
	VoteGranted bool
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	myIndex := rf.Log_if.last_index()
	myTerm := rf.Log_if.entry(myIndex).Term
	update_flag := (args.LastLogTerm == myTerm && args.LastLogIndex >= myIndex) || args.LastLogTerm > myTerm
	//if rf.currentTerm < args.Cand_Term {
	//	rf.state = Follower
	//	rf.currentTerm = args.Cand_Term
	//	rf.voteFor = args.Candidate_Id
	//	reply.VoteGranted = true
	//	rf.heartbeat = time.Now()
	//} else {
	//	reply.VoteGranted = false
	//	if rf.voteFor >= 0 {
	//		//reply.VoteGranted = false
	//		reply.Curr_Term = rf.currentTerm
	//		return
	//	}
	//	reply.Curr_Term = rf.currentTerm
	//}
	//DPrintf("%d update_condition is %v", rf.me, update_flag)
	if args.Cand_Term < rf.currentTerm {
		DPrintf("%d refuse vote for %d,because i have vote for %d", rf.me, args.Candidate_Id, rf.voteFor)
		reply.VoteGranted = false
	} else if (rf.voteFor == -1 || rf.voteFor == args.Candidate_Id) && update_flag {
		DPrintf("%d will vote for %d", rf.me, args.Candidate_Id)
		rf.voteFor = args.Candidate_Id
		reply.VoteGranted = true
		//rf.chanGrantVote <- true
		rf.heartbeat = time.Now()
		//rf.persist()
		rf.Convert_follower(args.Cand_Term)
	} else {
		DPrintf("%d refuse vote for %d,because i have voted for %d", rf.me, args.Candidate_Id, rf.voteFor)
		reply.VoteGranted = false
	}
	reply.Curr_Term = rf.currentTerm
}

type AppendEntries_Args struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log_info
	LeaderCommit int
}

type AppendEntries_Reply struct {
	Curr_Term      int
	Success        bool
	Conflict       bool
	Conflict_index int
	Conflict_Term  int
}

func (rf *Raft) Count_vote(reply *RequestVoteReply) {
	if reply.VoteGranted {
		rf.vote_num++
	}
	if rf.vote_num >= rf.majority {
		rf.heartbeat = time.Now()
		rf.Convert_Leader()
	}
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntries_Args, reply *AppendEntries_Reply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	reply.Success = false
	if rf.currentTerm <= args.Term {
		rf.heartbeat = time.Now()
		reply.Conflict_index = -1
		reply.Conflict_Term = -1
		//if rf.currentTerm < args.Term {
		rf.Convert_follower(args.Term)
		//}
		//if args.Entries == nil {
		//	reply.Success = true
		//	reply.Conflict = false
		//	if rf.commitIndex > rf.lastApplied {
		//		rf.check_point.Broadcast()
		//	}
		//	return
		//}
		//lastIndex := rf.Log_if.last_index()
		//if args.Entries != nil {
		//if lastIndex < args.PrevLogIndex || rf.Log_if.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		//	reply.Conflict = true
		//	reply.Success = false
		//	reply.Curr_Term = args.Term
		//	//log.Println("args.PrevLogTerm:", args.PrevLogTerm, "args.PrevLogTerm:", args.PrevLogTerm)
		//	//log.Println(rf.me, "lastIndex < args.PrevLogIndex:", lastIndex < args.PrevLogIndex, "lastIndex:", lastIndex, "args.PrevlogIndex:", args.PrevLogIndex)
		//	//log.Println("Is MY reply: Conflict", reply.Conflict)
		//	return
		//}
		//unmatch_idx := -1
		//for idx := range args.Entries {
		//	if rf.Log_if.last_index() < (args.PrevLogIndex+2+idx) ||
		//		rf.Log_if.entry(args.PrevLogIndex+1+idx).Term != args.Entries[idx].Term {
		//		// unmatch log found
		//		unmatch_idx = idx
		//		//log.Println("unmatch_idx:", unmatch_idx)
		//		break
		//	}
		//}
		//if unmatch_idx != -1 {
		//	// there are unmatch entries
		//	// truncate unmatch Follower entries, and apply Leader entries
		//	rf.Log_if.log_index = rf.Log_if.log_index[:(args.PrevLogIndex + 1 + unmatch_idx)]
		//	log.Println("here is cutup log:", rf.Log_if.log_index)
		//	rf.Log_if.log_index = append(rf.Log_if.log_index, args.Entries[unmatch_idx:]...)
		//	//log.Println(rf.me, "log_info:", rf.Log_if.log_index)
		//}
		//if args.LeaderCommit > rf.commitIndex {
		//	rf.commitIndex = Min(args.LeaderCommit, rf.Log_if.last_index())
		//}
		//if args.PrevLogIndex > rf.Log_if.last_index() || rf.Log_if.log_index[args.PrevLogIndex].Term != args.PrevLogTerm {
		if args.PrevLogIndex > rf.Log_if.last_index() || rf.Log_if.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
			//if args.PrevLogIndex < rf.Log_if.last_index() {
			//	rf.Log_if.log_index = rf.Log_if.log_index[0:args.PrevLogIndex] // delete the log in prevLogIndex and after it
			//	rf.persist()
			//}
			if args.PrevLogIndex < rf.Log_if.last_index() {
				rf.Log_if.log_index = rf.Log_if.log_index[0 : args.PrevLogIndex+1] // delete the log in prevLogIndex and after it
				rf.persist()
				if args.PrevLogTerm == rf.Log_if.log_index[args.PrevLogIndex].Term {
					rf.Log_if.log_index = append(rf.Log_if.log_index, args.Entries...)
					rf.persist()
					if args.LeaderCommit > rf.commitIndex {
						rf.commitIndex = Min(args.LeaderCommit, rf.Log_if.last_index())
					}
					if rf.commitIndex > rf.lastApplied {
						rf.check_point.Broadcast()
					}
					reply.Success = true
					reply.Conflict = false
					return
				}
			}
			if args.PrevLogIndex <= rf.Log_if.last_index() && rf.Log_if.log_index[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Conflict_Term = rf.Log_if.log_index[args.PrevLogIndex].Term
				for k, v := range rf.Log_if.log_index {
					if v.Term == rf.Log_if.log_index[args.PrevLogIndex].Term {
						reply.Conflict_index = k
						break
					}
				}
			} else {
				reply.Conflict_index = rf.Log_if.last_index()
				reply.Conflict_Term = -1
			}
			//for k, v := range args.Entries {
			//	if len(rf.Log_if.log_index) > k {
			//		if rf.Log_if.log_index[k].Term != v.Term || rf.Log_if.log_index[k].Command != v.Command {
			//			reply.Conflict_index = k
			//			//log.Println("conflict ")
			//			break
			//		}
			//	}
			//}
			reply.Conflict = true
			reply.Success = false
			reply.Curr_Term = args.Term
			//log.Println("return here")
			return
		}
		//rf.Log_if.log_index = rf.Log_if.log_index[0 : args.PrevLogIndex+1]
		//if reply.Conflict_index == -1 || reply.Conflict_Term == -1 {
		//	for append_index := rf.Log_if.last_index() + 1; append_index < len(args.Entries); append_index++ {
		//		rf.Log_if.log_index = append(rf.Log_if.log_index, args.Entries[append_index])
		//	}
		//}
		//rf.Log_if.log_index = append(rf.Log_if.log_index[0:args.PrevLogIndex+1], args.Entries...)
		Prev := args.PrevLogIndex - rf.Log_if.index0 + 1
		rf.Log_if.log_index = append(rf.Log_if.log_index[0:Prev], args.Entries...)
		rf.persist()
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.Log_if.last_index())
		}
		//log.Println(rf.me, "My Log", rf.Log_if.log_index)
		reply.Success = true
		reply.Conflict = false
		if rf.commitIndex > rf.lastApplied {
			rf.check_point.Broadcast()
		}
		//} else {
		//	if args.LeaderCommit > rf.commitIndex {
		//		rf.commitIndex = Min(args.LeaderCommit, rf.Log_if.last_index())
		//	}
		//	//rf.commitIndex = args.LeaderCommit
		//	rf.check_point.Broadcast()
		//	reply.Success = true
		//	reply.Conflict = false
		//}
	} else {
		reply.Conflict = true
		reply.Success = false
		reply.Curr_Term = rf.currentTerm
		return
	}
	reply.Curr_Term = args.Term
	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntries_Args, reply *AppendEntries_Reply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallsnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func Min(x int, y int) int {
	if x >= y {
		return y
	} else {
		return x
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		index = rf.Log_if.last_index()
		term = rf.currentTerm
		isLeader = false
		return index, term, isLeader
	} else {
		//fmt.Println("here is start")
		//log.Println(rf.me, " is leader ")
		//rf.lastApplied += 1
		term = rf.currentTerm
		//entry_tmp := map[int]interface{}{term: command}
		rf.Log_New.Term = term
		rf.Log_New.Command = command
		//rf.log = append(rf.log, entry_tmp)
		rf.Log_if.append(rf.Log_New)
		index = rf.Log_if.last_index()
		//isLeader = true
		rf.persist()
		//log.Println("new log:", rf.Log_if.log_index, "Index:", index)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Convert_follower(new_term int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.state == Follower {
		rf.state = Follower
		rf.voteFor = -1
		rf.currentTerm = new_term
		rf.vote_num = 0
	} else {
		rf.state = Follower
		rf.voteFor = -1
		rf.currentTerm = new_term
		rf.vote_num = 0
		rf.persist()
	}
	DPrintf("%d become a Follower", rf.me)
}

func (rf *Raft) Convert_Candidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.vote_num = 1
	rf.persist()
	//rf.Log_if.lastentry().Term = rf.currentTerm
	//log.Println(rf.Log_if.lastentry().Term)
	DPrintf("%d become a Candidate", rf.me)
}
func (rf *Raft) Convert_Leader() {
	rf.state = Leader
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.Log_if.last_index() + 1
		//rf.matchIndex[peer] = rf.Log_if.last_index()
	}
	//rf.persist()
	//rf.Log_if.lastentry().Term = rf.currentTerm
	//log.Println(rf.Log_if.lastentry().Term)
	//log.Println(rf.me, "become a Leader,log_info:", rf.Log_if.log_index)
	DPrintf("%d become a Leader", rf.me)
	//log.Println(rf.me, "become a Leader")
	go rf.Send_heartbeat()
}
func (rf *Raft) kickoff() {
	var wg sync.WaitGroup
	Add_count := len(rf.peers) - 1
	wg.Add(Add_count)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.mu.Unlock()
	//rf.heartbeat = time.Now()
	rf.Convert_Candidate()
	args := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.Log_if.last_index(),
		rf.Log_if.entry(rf.Log_if.last_index()).Term,
	}
	//log.Println(rf.me, "kick off: LastLogIndex:", rf.Log_if.last_index(), "LastLogTerm:", rf.Log_if.entry(rf.Log_if.last_index()).Term)
	//rf.mu.Unlock()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			go func(peer int) {
				defer wg.Done()
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(peer, args, reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				//DPrintf("is here")
				if reply.Curr_Term > rf.currentTerm {
					rf.Convert_follower(reply.Curr_Term)
				}
				if rf.state == Leader {
					rf.mu.Unlock()
					return
				}
				rf.Count_vote(reply)
				//rf.check_vote += 1
				rf.mu.Unlock()
			}(peer)
		}
	}
}

func (rf *Raft) Send_heartbeat() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			//log.Println(rf.me, "the Term_num:", args_AppE.Term)
			for peer := 0; peer < len(rf.peers); peer++ {
				if peer != rf.me {
					go func(peer int) {
						rf.mu.Lock()
						next := rf.nextIndex[peer]
						if next <= rf.Log_if.start() {
							next = rf.Log_if.start() + 1
						}
						if next-1 > rf.Log_if.last_index() {
							next = rf.Log_if.last_index()
						}
						args_AppE := &AppendEntries_Args{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: next - 1,
							PrevLogTerm:  rf.Log_if.entry(next - 1).Term,
							//Entries:      make([]Log_info, len(rf.Log_if.log_index[next:])),
							Entries:      make([]Log_info, len(rf.Log_if.slice(next))),
							LeaderCommit: rf.commitIndex,
						}
						//if rf.Log_if.last_index()==
						copy(args_AppE.Entries, rf.Log_if.slice(next))
						//copy(args_AppE.Entries, rf.Log_if.log_index)
						//log.Println("send", args_AppE.Entries, "to", peer)
						reply_AppE := &AppendEntries_Reply{}
						//if len(args_AppE.Entries) > 0 {
						rf.matchIndex[rf.me] = args_AppE.PrevLogIndex + len(args_AppE.Entries)
						rf.nextIndex[rf.me] = args_AppE.PrevLogIndex + len(args_AppE.Entries) + 1
						//rf.matchIndex[rf.me] = args_AppE.PrevLogIndex + len(rf.Log_if.slice(next))
						//rf.nextIndex[rf.me] = args_AppE.PrevLogIndex + len(rf.Log_if.slice(next)) + 1
						//}
						rf.mu.Unlock()
						rf.sendAppendEntries(peer, args_AppE, reply_AppE)
						//rf.mu.Lock()
						rf.Reply_AppE_handler(peer, args_AppE, reply_AppE)
						//rf.mu.Unlock()
					}(peer)
				}
			}
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		} else {
			rf.mu.Unlock()
			break
		}
	}
}
func (rf *Raft) Reply_AppE_handler(peer int, args_AppE *AppendEntries_Args, reply *AppendEntries_Reply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Curr_Term > rf.currentTerm {
		rf.Convert_follower(reply.Curr_Term)
		rf.heartbeat = time.Now()
		//rf.persist()
	} else if args_AppE.Term == reply.Curr_Term {
		if reply.Success {
			if args_AppE.Entries == nil {
				return
			}
			//log.Println("here here ")
			//next := rf.nextIndex[peer]
			if len(args_AppE.Entries) > 0 {
				rf.matchIndex[peer] = args_AppE.PrevLogIndex + len(args_AppE.Entries)
				//rf.matchIndex[peer] = args_AppE.PrevLogIndex + len(rf.Log_if.slice(next))
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				//log.Println("peer:", peer, "rf.matchIndex", rf.matchIndex[peer])
			}
		} else if reply.Conflict {
			//log.Println(peer, "reconnect follower will come here")
			//prevIndex := args_AppE.PrevLogIndex
			//for prevIndex > 0 && rf.Log_if.entry(prevIndex).Term == args_AppE.PrevLogTerm {
			//	prevIndex--
			//}
			//rf.nextIndex[peer] = prevIndex + 1
			//if reply.Conflict_Term != -1 {
			//	for k, v := range rf.Log_if.log_index {
			//		if v.Term == reply.Conflict_Term {
			//			rf.nextIndex[peer] = k + 1
			//			reply.Conflict_Term = -1
			//			break
			//		}
			//	}
			//	if reply.Conflict_Term != -1 {
			//		rf.nextIndex[peer] = reply.Conflict_index
			//	}
			//} else if reply.Conflict_index != -1 {
			//	rf.nextIndex[peer] = reply.Conflict_index + 1
			//}
			//
			//rf.Quick_backup(peer)
			if reply.Conflict_Term != -1 {
				//prevIndex := args_AppE.PrevLogIndex
				//for prevIndex > 0 && rf.Log_if.log_index[prevIndex].Term == args_AppE.PrevLogTerm {
				//	prevIndex--
				//}
				//rf.nextIndex[peer] = prevIndex + 1
				for k, v := range rf.Log_if.log_index {
					if v.Term == reply.Conflict_Term {
						rf.nextIndex[peer] = k + 1
						reply.Conflict_Term = -1
					}
				}
				if reply.Conflict_Term != -1 {
					rf.nextIndex[peer] = reply.Conflict_index
				}
			} else {
				rf.nextIndex[peer] = reply.Conflict_index
			}
		} else {
			rf.nextIndex[peer] = rf.nextIndex[peer] - 1
			rf.send_one(peer)
		}
	}
	rf.Leader_commit()
}
func (rf *Raft) Leader_commit() {
	if rf.state != Leader {
		return
	}
	//for k, v := range rf.matchIndex {
	//
	//	//log.Println("loop here condition:", v == rf.lastApplied && rf.lastApplied >= 1)
	//	if v == rf.lastApplied && rf.lastApplied >= 1 {
	//		cout += 1
	//		DPrintf("count num: %d", cout)
	//	}
	//}
	//if cout >= rf.majority {
	//	DPrintf("leader commitIndex ++")
	//	rf.commitIndex += 1
	//	rf.checkpoint = true
	//	rf.check_point.Broadcast()
	//}
	start := rf.commitIndex
	if start == 0 {
		start = 1
	}
	for index := start; index <= len(rf.Log_if.log_index); index++ {
		if index == 0 {
			continue
		}
		cout := 1
		//log.Println("index:", index)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] == index {
				cout += 1
				//log.Println(cout)
			}
		}
		//log.Println("is true?", cout >= rf.majority)
		if cout >= rf.majority && rf.Log_if.entry(index).Term == rf.currentTerm {
			rf.commitIndex = index
			rf.checkpoint = true
		}
	}
	rf.check_point.Broadcast()
	// think a little
	//for peer:=0;peer<len(rf.peers);peer++{
	//	if peer!=rf.me{
	//		args:=&AppendEntries_Args{
	//			Entries: nil,
	//			LeaderId: rf.me,
	//			PrevLogIndex: rf.Log_if.last_index(),
	//			PrevLogTerm: rf.Log_if.index0,
	//		}
	//		reply:=&AppendEntries_Reply{}
	//		go rf.sendAppendEntries(peer,args,reply)
	//	}
	//}

}

func (rf *Raft) send_one(peer int) {

}
func (rf *Raft) Quick_backup(peer int) {
	PrevLogIndex := rf.matchIndex[peer]
	//var entires []Log_info
	args_AppE := &AppendEntries_Args{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: PrevLogIndex,
		PrevLogTerm:  rf.Log_if.entry(PrevLogIndex).Term,
		Entries:      make([]Log_info, rf.Log_if.last_index()-PrevLogIndex),
		LeaderCommit: rf.commitIndex,
	}
	reply_AppE := &AppendEntries_Reply{}
	//ok := rf.sendAppendEntries(peer, args_AppE, reply_AppE)
	//if ok && reply_AppE.Success {
	//for i := rf.matchIndex[peer]; i < rf.Log_if.last_index(); i++ {
	//	entires = append(entires,rf.Log_if.entry(i))
	//}
	//log.Println("peer:", peer, "PrevIndex:", PrevLogIndex)
	copy(args_AppE.Entries, rf.Log_if.slice(PrevLogIndex+1))
	//}
	//args_AppE.Entries = entires
	//log.Println("peer:", peer, "backup log:", args_AppE.Entries)
	rf.sendAppendEntries(peer, args_AppE, reply_AppE)
	if reply_AppE.Success {
		rf.matchIndex[peer] = args_AppE.PrevLogIndex + len(args_AppE.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
}
func (rf *Raft) Start_Election() {
	for {
		electionTimeout := rf.ElectionInterval + rand.Intn(150)
		//fmt.Println(electionTimeout)
		start_time := time.Now()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader {
			if rf.heartbeat.Before(start_time) {
				rf.heartbeat = time.Now()
				go rf.kickoff()
				//DPrintf("%d become a Candidate", rf.me)
			}
		}
		if rf.state == Candidate {
			rf.voteFor = -1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Check_event(applyCh chan ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = 0
	if rf.lastApplied+1 <= rf.Log_if.start() {
		rf.lastApplied = rf.Log_if.start()
	}
	for !rf.killed() {
		//rf.mu.Lock()
		//log.Println("rf.checkpoint && rf.lastApplied < rf.commitIndex:", rf.checkpoint && rf.lastApplied < rf.commitIndex)
		if rf.waitsnap != nil {
			//rf.mu.Unlock()
			//log.Println(rf.me, "send message:")
			//args := <-rf.argsch
			apply := ApplyMsg{}
			apply.SnapshotValid = true
			apply.Snapshot = rf.waitsnap
			apply.SnapshotTerm = rf.waitTerm
			apply.SnapshotIndex = rf.waitIndex
			//rf.mu.Unlock()
			log.Println(rf.me, "send snapshot message:", apply)
			applyCh <- apply
			rf.mu.Lock()
			apply.SnapshotValid = false
			rf.waitsnap = nil
			log.Println("after send message")
		} else if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.Log_if.last_index() &&
			rf.lastApplied+1 > rf.Log_if.start() {
			//log.Println("come here")
			//for rf.lastApplied < rf.commitIndex {
			apply := ApplyMsg{}
			log.Println(rf.me, "lastapplied:", rf.lastApplied)
			apply.CommandValid = true
			rf.lastApplied += 1
			//if rf.lastApplied <= rf.commitIndex {
			apply.CommandIndex = rf.lastApplied
			apply.Command = rf.Log_if.entry(rf.lastApplied).Command
			log.Println(rf.me, "send apply message:", apply.Command)
			rf.mu.Unlock()
			applyCh <- apply
			rf.mu.Lock()
			apply.CommandValid = false
			rf.checkpoint = false
			//}
			//}
			//rf.success = false
			//rf.mu.Unlock()
		} else {
			//rf.mu.Unlock()
			rf.checkpoint = false
			//log.Println("wait here")
			rf.check_point.Wait()
			//log.Println("wake up")
		}
		//log.Println("wait here")
	}
}
func mkLogEmpty() Log {
	return Log{make([]Log_info, 1), 0}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peer_num = len(peers)
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	//rf.check_vote = 1
	//rf.Convert_follower(rf.currentTerm)
	rf.state = Follower
	rf.voteFor = -1
	rf.vote_num = 0
	rf.majority = rf.peer_num/2 + 1
	rf.ElectionInterval = 300
	rf.lastApplied = 0
	rf.checkpoint = false
	rf.commitIndex = 0
	//rf.chanvote = make([]chan bool, len(peers))
	rf.check_point = sync.NewCond(&rf.mu)
	rf.wg = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	//for peer := 0; peer < len(peers); peer++ {
	//	rf.nextIndex[peer] = 1
	//}
	rf.snapshort_index = 0
	rf.snapshortflag = false
	rf.Log_if = mkLogEmpty()
	rf.argsch = make(chan *InstallSnapshotArgs)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//log.Println(rf.Log_if.log_index)

	go rf.Start_Election()
	go rf.Check_event(applyCh)
	//go rf.Receive()
	return rf
}
