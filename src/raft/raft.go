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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"log"

	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
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

type Log struct {
        Term int
        Command interface{}
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
	isLeader bool
        isCandidate bool
        isFollower bool
        currentTerm int
        votedFor int
        lastLogIndex int
        lastLogTerm int
	logNumber int
        logs []Log
	snapshot []byte
        matchMaps map[int]int
        applyCond *sync.Cond
	replicatorCond []*sync.Cond
        applyChan chan ApplyMsg
        collect_vote int
        rest_start_time time.Time
        election_start_time time.Time

        commitIndex int
        lastApplied int

        nextIndex []int
        matchIndex []int

        min_reelection_time int
        check_interval int
        heartbeat_time int

	startIndex int
	lastIncludedIndex int
	lastIncludedTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
        isleader = rf.isLeader
        return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	 w := new(bytes.Buffer)
        e := labgob.NewEncoder(w)
        e.Encode(rf.currentTerm)
        e.Encode(rf.votedFor)
        e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
        data := w.Bytes()
        rf.persister.Save(data, rf.snapshot)
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
	r := bytes.NewBuffer(data)
        d := labgob.NewDecoder(r)
        var term int
        var votedFor int
        var logs []Log
	var lastIncludedIndex int
	var lastIncludedTerm int
        if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
	d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
                log.Printf("Error when Decode persist state\n")
        } else {
                rf.currentTerm = term
                rf.votedFor = votedFor
                rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		Debug(dLog, "Term %d, server %d, lastIncludedIndex %d\n", rf.currentTerm, rf.me, rf.lastIncludedIndex)
		rf.lastIncludedTerm = lastIncludedTerm
                rf.logNumber = rf.lastIncludedIndex + 1 + len(rf.logs)
		rf.lastLogIndex = rf.logNumber - 1
		rf.startIndex = rf.lastIncludedIndex + 1
		rf.lastLogTerm = rf.lastIncludedTerm
		if len(logs) > 0 {
			rf.lastLogTerm = rf.logs[rf.lastLogIndex - rf.startIndex].Term
		}
		rf.snapshot = rf.persister.ReadSnapshot()
        }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index-- // Our index start from 0
	rf.snapshot = snapshot
	Debug(dSnapshot, "Term %d, server %d snapshot %v\n", rf.currentTerm, rf.me, rf.snapshot)
	rf.lastIncludedIndex = index
	if rf.lastIncludedIndex >= 0 {
		rf.lastIncludedTerm = rf.logs[rf.lastIncludedIndex - rf.startIndex].Term
	}
	rf.logs = rf.logs[rf.lastIncludedIndex + 1 - rf.startIndex :]
	rf.startIndex = rf.lastIncludedIndex + 1
	rf.persist()

}

func (rf *Raft) applyGoroutine() {
        for {
                if rf.killed() {
			close(rf.applyChan)
                        return
                }
                rf.applyCond.L.Lock()
                rf.applyCond.Wait()
		if rf.lastApplied < rf.lastIncludedIndex {
			Debug(dSnapshot, "Term %d, server %d write snapshot\n", rf.currentTerm, rf.me)
			msg := ApplyMsg{false, nil, -1, true, rf.snapshot, rf.lastIncludedTerm, rf.lastIncludedIndex + 1}
			rf.applyCond.L.Unlock()
			rf.applyChan <- msg
			rf.applyCond.L.Lock()
			rf.lastApplied = rf.lastIncludedIndex
		}
                if rf.lastApplied < rf.commitIndex{
                        var apply_msgs []ApplyMsg
                        for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
                                apply_msg := ApplyMsg{true, rf.logs[i - rf.startIndex].Command, i + 1, false, nil, 0, 0}
                                rf.lastApplied++
                                apply_msgs = append(apply_msgs, apply_msg)
                        }
                        rf.applyCond.L.Unlock()
                        for _, msg := range apply_msgs {
				Debug(dApply, "Term %d, server %d apply %v in index %d\n", rf.currentTerm, rf.me, msg.Command, msg.CommandIndex - 1)
                                rf.applyChan <- msg
                        }
                } else {
                        rf.applyCond.L.Unlock()
                }
        }
}

func (rf *Raft) replicator(server int) {
	rf.replicatorCond[server].L.Lock()
	defer rf.replicatorCond[server].L.Unlock()
	for rf.killed() == false {
		Debug(dLog, "server %d for server %d sleep\n", rf.me, server)
		if (rf.needReplicate(server)) {
			rf.sendAppendEntriesForOneServer(server)
		} else {
			rf.replicatorCond[server].Wait()
		}
	}
}

func (rf *Raft) needReplicate(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isLeader && rf.nextIndex[server] < rf.lastLogIndex
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
        CandidateId int
        LastLogIndex int
        LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
        VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
        defer rf.mu.Unlock()

	Debug(dVote, "Term %d, server %d receive vote request from server %d, candidate term is %d, lastLogIndex is %d, lastLogTerm %d\n", rf.currentTerm, rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
        if args.Term < rf.currentTerm {
                reply.Term = rf.currentTerm
                reply.VoteGranted = false
                return
        }

	granted := false
        if args.Term > rf.currentTerm {
                rf.currentTerm = args.Term
                rf.votedFor = -1
                rf.isLeader = false
                rf.isCandidate = false
                rf.isFollower = true
        }

	if (rf.votedFor == -1) || (rf.votedFor == args.CandidateId) {
                if rf.logNumber == 0 {
                        granted = true
                } else if rf.lastLogTerm < args.LastLogTerm {
                        granted = true
                } else if rf.lastLogTerm == args.LastLogTerm && rf.logNumber <= args.LastLogIndex + 1 {
                        granted = true
                } else {
			Debug(dVote, "Term %d, server %d, last log term %d, args last log term %d\n", rf.currentTerm, rf.me, rf.lastLogTerm, args.LastLogTerm)
                        granted = false
                }
        }

        if granted {
                reply.Term = rf.currentTerm
                reply.VoteGranted = true
                rf.votedFor = args.CandidateId
		rf.persist()
                rf.rest_start_time = time.Now()
                reelection_time := time.Duration(rf.min_reelection_time) * time.Millisecond + time.Duration(rand.Int() % 300) * time.Millisecond
                rf.election_start_time = rf.rest_start_time.Add(reelection_time)
		Debug(dVote, "Term %d, server %d granted vote to server %d\n", rf.currentTerm, rf.me, args.CandidateId)
                return
        }

	Debug(dVote, "Term %d, server %d refuse granted vote to server %d\n", rf.currentTerm, rf.me, args.CandidateId)
	rf.persist()
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
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
	rf.mu.Lock()
        defer rf.mu.Unlock()
        index := -1
        term := -1
        isLeader := false

        // Your code here (2B).
        if !rf.isLeader {
                return index, term, isLeader
        }

        log := Log{rf.currentTerm, command}
        rf.logs = append(rf.logs, log)
        rf.logNumber++
	rf.lastLogIndex = rf.logNumber - 1
        rf.matchMaps[rf.lastLogIndex] = 1
        rf.lastLogTerm = rf.currentTerm

	Debug(dStart, "Term %d, server %d start %v in index %d\n", rf.currentTerm, rf.me, command, rf.lastLogIndex)
        index = rf.lastLogIndex
        term = rf.currentTerm
        isLeader = true

	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.replicatorCond[i].Signal()
		}
	}
        return index + 1, term, isLeader
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

type AppendEntriesArgs struct {
        Term int
        LeaderId int
        PrevLogIndex int
        PrevLogTerm int
        Entries  []Log
        LeaderCommit int
}

type AppendEntriesReply struct {
        Term int
        Success bool
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
        rf.mu.Lock()
        defer rf.mu.Unlock()
	Debug(dAppend, "Term %d, server %d recieve append entries from leader %d\n", rf.currentTerm, rf.me, args.LeaderId)
        if (args.Term < rf.currentTerm) {
                reply.Term  = rf.currentTerm
                reply.Success = false
		Debug(dAppend, "Term %d, server %d refuse append entries from leader %d because small term %d\n", rf.currentTerm, rf.me, args.LeaderId, args.Term)
                return
        }

        if (args.Term >= rf.currentTerm) {
                rf.currentTerm = args.Term
                rf.isLeader = false
                rf.isCandidate = false
                rf.isFollower = true
		rf.persist()
                rf.rest_start_time = time.Now()
                reelection_time := time.Duration(rf.min_reelection_time) * time.Millisecond + time.Duration((rand.Int() % 300)) * time.Millisecond
                rf.election_start_time = rf.rest_start_time.Add(reelection_time)
        }

	Debug(dAppend, "Term %d, server %d recieve args prevLogIndex %d, rf lastIncludedIndex %d, len logs %d\n", rf.currentTerm, rf.me, args.PrevLogIndex, rf.lastIncludedIndex, len(rf.logs))
        if (rf.logNumber <= args.PrevLogIndex) || (args.PrevLogIndex < rf.lastIncludedIndex) || (args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm) || ((args.PrevLogIndex > rf.lastIncludedIndex) && (rf.logs[args.PrevLogIndex - rf.startIndex].Term != args.PrevLogTerm)) {
                reply.Term = rf.currentTerm
                reply.Success = false
		Debug(dAppend, "Term %d, server %d refuse append entries from leader %d because unmatch log and term\n", rf.currentTerm, rf.me, args.LeaderId)
                if rf.logNumber <= args.PrevLogIndex {
			Debug(dAppend, "Term %d, server %d log number %d is too small for prev log index %d\n", rf.currentTerm, rf.me, rf.logNumber, args.PrevLogIndex)
		} else {
			Debug(dAppend, "Term %d, server %d recieve prev log index is %d, prev log term %d\n", rf.currentTerm, rf.me, args.PrevLogIndex, args.PrevLogTerm)
		}
		return
        }

        unmatch_index := -1
        log_index := args.PrevLogIndex + 1
        entry_index := 0
        for ; (log_index < rf.logNumber) && (entry_index < len(args.Entries)); log_index, entry_index = log_index + 1, entry_index + 1 {
                if rf.logs[log_index - rf.startIndex].Term != args.Entries[entry_index].Term {
                        unmatch_index = entry_index
                        break
                }
        }

	Debug(dAppend, "Term %d, server %d unmatch index %d", rf.currentTerm, rf.me, unmatch_index)
        if unmatch_index != -1 {
                rf.logs = append(rf.logs[:args.PrevLogIndex + 1 + unmatch_index - rf.startIndex], args.Entries[unmatch_index:]...)
        } else {
                if entry_index < len(args.Entries) {
                        rf.logs = append(rf.logs, args.Entries[entry_index:]...)
                }
        }
	rf.persist()

	rf.logNumber = len(rf.logs) + rf.startIndex
        if len(rf.logs) > 0 {
                rf.lastLogIndex = rf.logNumber - 1
                rf.lastLogTerm = rf.logs[rf.lastLogIndex - rf.startIndex].Term
        }

        if args.LeaderCommit > rf.commitIndex {
                rf.commitIndex = args.LeaderCommit
                rf.applyCond.Signal()
        }

        reply.Term = rf.currentTerm
        reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
        ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
        return ok
}

type InstallSnapshotArgs struct {
        Term int
        LeaderId int
        LastIncludedIndex int
        LastIncludedTerm int
        Data []byte
}

type InstallSnapshotReply struct {
        Term int
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
        rf.mu.Lock()
	defer rf.mu.Unlock()
        if args.Term < rf.currentTerm {
                reply.Term = rf.currentTerm
                return
        }

        if args.Term >= rf.currentTerm {
                rf.currentTerm = args.Term
                rf.isLeader = false
                rf.isCandidate = false
		rf.isFollower = true
		rf.persist()
                rf.rest_start_time = time.Now()
                reelection_time := time.Duration(rf.min_reelection_time) * time.Millisecond + time.Duration((rand.Int() % 300)) * time.Millisecond
                rf.election_start_time = rf.rest_start_time.Add(reelection_time)
        }

        reply.Term = rf.currentTerm

	Debug(dAppend, "Term %d, server %d recieve install snapshot args lastIncludedIndex %d, args lastIncludedTerm %d\n", rf.currentTerm, rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
	if args.LastIncludedIndex < rf.startIndex {
		return
	}
	if args.LastIncludedIndex >= rf.logNumber {
                rf.logs = nil
        } else {
                if rf.logs[args.LastIncludedIndex - rf.startIndex].Term == args.LastIncludedTerm {
                        rf.logs = rf.logs[args.LastIncludedIndex + 1 - rf.startIndex:]
                } else {
                        rf.logs = nil
                }
        }

        rf.snapshot = args.Data
        rf.lastIncludedIndex = args.LastIncludedIndex
        rf.lastIncludedTerm = args.LastIncludedTerm
	rf.startIndex = args.LastIncludedIndex + 1
	rf.logNumber = args.LastIncludedIndex + 1 + len(rf.logs)
	rf.persist()
        rf.applyCond.Signal()

        return
}

func (rf *Raft) sendInstallSnapshotRPC(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshot}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshotHandler", &args, &reply)
	rf.mu.Lock()
	if ok {
		Debug(dSnapshot, "Term %d, leader %d recieve reply from server %d\n", rf.currentTerm, rf.me, server)
		if reply.Term > rf.currentTerm {
			rf.isLeader = false
			rf.isCandidate = false
			rf.isFollower = true
			rf.currentTerm = reply.Term
			rf.persist()
			rf.mu.Unlock()
			return
		} else {
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			Debug(dSnapshot, "Term %d, leader %d update server %d nextIndex to %d\n", rf.currentTerm, rf.me, server, rf.nextIndex[server])
		}
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntriesForOneServer(server int) {
        rf.mu.Lock()
        if !rf.isLeader {
                rf.mu.Unlock()
                return
        }

	Debug(dAppend, "Term %d, leader %d, last log index %d, next index %d for server %d\n", rf.currentTerm, rf.me, rf.lastLogIndex, rf.nextIndex[server], server)
        if rf.nextIndex[server] <= rf.lastIncludedIndex {
		go rf.sendInstallSnapshotRPC(server)
		rf.mu.Unlock()
		return
	}
	prev_log_index := rf.nextIndex[server] - 1
        if rf.nextIndex[server] > rf.lastLogIndex {
                prev_log_index = rf.lastLogIndex
        }
        prev_log_term := rf.lastIncludedTerm
        if prev_log_index > rf.lastIncludedIndex {
                prev_log_term = rf.logs[prev_log_index - rf.startIndex].Term
        }

        args := AppendEntriesArgs{rf.currentTerm, rf.me, prev_log_index, prev_log_term, rf.logs[prev_log_index + 1 - rf.startIndex:], rf.commitIndex}
        reply := AppendEntriesReply{}
        rf.mu.Unlock()
	Debug(dAppend, "Term %d, leader %d send a append entries to server %d, prev log index is %d, prev log term is %d, commmit index is %d\n", args.Term, rf.me, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
        rpc_return := rf.sendAppendEntries(server, &args, &reply)
        rf.mu.Lock()
        if rpc_return {
                if reply.Term < rf.currentTerm {
                        rf.mu.Unlock()
                        return
                }
                if reply.Term > rf.currentTerm {
                        rf.isLeader = false
                        rf.isCandidate = false
                        rf.isFollower = true
                        rf.currentTerm = reply.Term
			rf.persist()
                        rf.mu.Unlock()
                        return
                }
                if reply.Success {
                        rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
			if rf.nextIndex[server] > rf.logNumber {
				rf.nextIndex[server] = rf.logNumber
			}
			Debug(dAppend, "Term %d, leader %d receive success reply from server %d", rf.currentTerm, rf.me, server)
                        prev_match_index := rf.matchIndex[server]
                        rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
                        for i := prev_match_index + 1; i <= rf.matchIndex[server]; i++ {
                                rf.matchMaps[i]++
                                if (rf.matchMaps[i] >= (len(rf.peers) / 2 + 1)) && (i > rf.commitIndex) {
					if rf.logs[i - rf.startIndex].Term == rf.currentTerm {
						rf.commitIndex = i
                                                rf.applyCond.Signal()
					}
                                }
                        }
                        rf.mu.Unlock()
                        return
                }

		if rf.nextIndex[server] > 0 {
                        rf.nextIndex[server] = rf.nextIndex[server] - 1
			Debug(dAppend, "Term %d, leader %d update server %d next index to %d", rf.currentTerm, rf.me, server, rf.nextIndex[server])
                }
        }
        rf.mu.Unlock()
}

func (rf *Raft) heartbeatGoroutine() {

        for {
		rf.mu.Lock()
                for !rf.isLeader {
			rf.mu.Unlock()
                        time.Sleep(time.Duration(rf.check_interval) * time.Millisecond)
			rf.mu.Lock()
                }

                for index, _ := range rf.peers {
                        if index == rf.me {
                                continue
                        }
                        go rf.sendAppendEntriesForOneServer(index)
                }

		rf.mu.Unlock()
                time.Sleep(time.Duration(rf.heartbeat_time) * time.Millisecond)
        }

}

func (rf *Raft) requestVoteGoroutine(server int) {
        rf.mu.Lock()
        if !rf.isCandidate {
                rf.mu.Unlock()
                return
        }

        args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex, rf.lastLogTerm}
        reply := RequestVoteReply{}
        rf.mu.Unlock()
	Debug(dVote, "Term %d, server %d send a vote request to server %d\n", args.Term, rf.me, server)
        rpc_return := rf.sendRequestVote(server, &args, &reply)
        if !rpc_return {
                return
        }
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if reply.Term > rf.currentTerm {
                rf.isCandidate = false
                rf.isFollower = true
                rf.currentTerm = reply.Term
		rf.persist()
                return
        }

        if reply.Term < rf.currentTerm {
                return
        }

        if reply.VoteGranted {
                rf.collect_vote++
        }
        if rf.collect_vote >= (len(rf.peers) / 2 + 1) {
                if !rf.isLeader {
                        rf.isLeader = true
                        rf.isCandidate = false
                }
        }
}

func (rf *Raft) election_poller() {
        reelection_time := time.Duration(rf.min_reelection_time) * time.Millisecond + time.Duration(rand.Int() % 300) * time.Millisecond
        rf.rest_start_time = time.Now()
        rf.election_start_time = rf.rest_start_time.Add(reelection_time)
        for {
		rf.mu.Lock()
                if rf.killed() {
			rf.mu.Unlock()
                        return
                }
                if rf.isLeader {
			rf.mu.Unlock()
                        time.Sleep(time.Duration(rf.check_interval) * time.Millisecond)
			continue
                }

                if rf.isFollower {
                        now_time := time.Now()
                        if (now_time.After(rf.election_start_time)) {
                                rf.isFollower = false
                                rf.isCandidate = true
                                rf.currentTerm = rf.currentTerm + 1
                                rf.votedFor = rf.me
				rf.persist()
                                rf.collect_vote = 1
				Debug(dVote, "Term %d, server %d convert to candidate, start an election\n", rf.currentTerm, rf.me)
                                for index, _ := range rf.peers {
                                        if index == rf.me {
                                                continue
                                        }
                                        go rf.requestVoteGoroutine(index)
                                }
                                reelection_time := time.Duration(rf.min_reelection_time) * time.Millisecond + time.Duration(rand.Int() % 300) * time.Millisecond
                                rf.rest_start_time = time.Now()
                                rf.election_start_time = rf.rest_start_time.Add(reelection_time)
                        } else {
				rf.mu.Unlock()
                                time.Sleep(time.Duration(rf.check_interval) * time.Millisecond)
				rf.mu.Lock()
                        }
                }

		if rf.isCandidate {
                        now_time := time.Now()
                        if (now_time.After(rf.election_start_time)) {
                                rf.collect_vote = 1
                                rf.currentTerm = rf.currentTerm + 1
				rf.persist()
				Debug(dVote, "Term %d, server %d election timeout, start an election\n", rf.currentTerm, rf.me)
                                for index, _ := range rf.peers {
                                        if index == rf.me {
                                                continue
                                        }
                                        go rf.requestVoteGoroutine(index)
                                }
                                reelection_time := time.Duration(rf.min_reelection_time) * time.Millisecond + time.Duration(rand.Int() % 300) * time.Millisecond
                                rf.rest_start_time = time.Now()
                                rf.election_start_time = rf.rest_start_time.Add(reelection_time)
                        } else {
				rf.mu.Unlock()
                                time.Sleep(time.Duration(rf.check_interval) * time.Millisecond)
				rf.mu.Lock()
                        }
                }
		rf.mu.Unlock()
        }
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.min_reelection_time = 300
        rf.check_interval = 10
        rf.heartbeat_time = 120
        rf.currentTerm = 0
        rf.votedFor = -1
        rf.commitIndex = -1
        rf.lastApplied = -1
	rf.isFollower = true
        rf.isLeader = false
        rf.isCandidate = false
        rf.matchMaps = make(map[int]int)
        rf.lastLogIndex = -1
	rf.lastLogTerm = -1
	rf.logNumber = 0
        rf.applyChan = applyCh
        rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1
	rf.startIndex = 0
	rf.replicatorCond = make([]*sync.Cond, len(rf.peers))
        for i := 0; i < len(rf.peers); i++ {
                rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex + 1)
                rf.matchIndex = append(rf.matchIndex, -1)
        }

        rf.readPersist(persister.ReadRaftState())
	Debug(dBoot, "Term %d, server %d start boot with logs len %d\n", rf.currentTerm, rf.me, len(rf.logs))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	go rf.election_poller()
        go rf.heartbeatGoroutine()
        go rf.applyGoroutine()

	return rf
}
