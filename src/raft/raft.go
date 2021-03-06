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
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

func init() {
	log_file, err := os.OpenFile("raft.log", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Fail to open log file!", err)
	}
	log.SetOutput(log_file)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
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

const election_timeout_lb int = 300
const election_timeout_up int = 500

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	heartbeat_channel chan int
	vote_channel      chan int
	applyCh           chan ApplyMsg

	// These three states will be persistent
	current_term        int
	voted_for           int
	log                 []LogEntry
	last_included_index int // Snapshot info
	last_included_term  int // Snapshot info

	state            int // 0 for follower, 1 for candidate, 2 for leader
	election_timeout int
	recent_heartbeat bool
	vote_count       int
	commit_index     int
	last_applied     int
	next_index       []int
	match_index      []int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

/*
 @brief Return currentTerm and whether this server
        believes it is the leader.
*/
func (rf *Raft) GetState() (int, bool) {
	var term int = -1
	var isleader bool

	rf.mu.Lock()
	term = rf.current_term
	isleader = (rf.state == 2)
	rf.mu.Unlock()

	return term, isleader
}

/*
 @brief Save Raft's persistent state to stable storage,
        where it can later be retrieved after a crash and restart.
 @Note Thread unsafe, need outside lock,
*/
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.current_term)
	encoder.Encode(rf.voted_for)
	encoder.Encode(rf.log)
	encoder.Encode(rf.last_included_index)
	encoder.Encode(rf.last_included_term)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.current_term)
	encoder.Encode(rf.voted_for)
	encoder.Encode(rf.log)
	encoder.Encode(rf.last_included_index)
	encoder.Encode(rf.last_included_term)
	state_data := buffer.Bytes()

	rf.persister.SaveStateAndSnapshot(state_data, snapshot)
}

/*
 @brief Restore previously persisted state.
*/
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var current_term_r int
	var voted_for_r int
	var log_r []LogEntry
	var last_included_index int
	var last_included_term int

	rf.mu.Lock()
	if decoder.Decode(&current_term_r) != nil ||
		decoder.Decode(&voted_for_r) != nil ||
		decoder.Decode(&log_r) != nil ||
		decoder.Decode(&last_included_index) != nil ||
		decoder.Decode(&last_included_term) != nil {
		log.Fatalf("Server %v failed to recover from previously persisted state!", rf.me)
	} else {
		rf.current_term = current_term_r
		rf.voted_for = voted_for_r
		rf.log = log_r
		rf.last_included_index = last_included_index
		rf.last_included_term = last_included_term
		rf.last_applied = last_included_index
		rf.commit_index = last_included_index
		log.Printf("Server %v recoverd from persisted state: Current term %v, Voted for %v...", rf.me, rf.current_term, rf.voted_for)
	}
	rf.mu.Unlock()
}

//
// A trivial interface
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index > rf.last_included_index {
		new_log := []LogEntry{}
		if index-rf.last_included_index < len(rf.log) {
			new_log = append(new_log, rf.log[index-rf.last_included_index:len(rf.log)]...)
		}

		rf.last_included_term = rf.log[index-rf.last_included_index-1].Term
		rf.last_included_index = index
		rf.log = new_log

		log.Printf("Snapshot is created for server %v: lastIncludedIndex = %v, lastIncludedTerm = %v... (Current term: %v)",
			rf.me, rf.last_included_index, rf.last_included_term, rf.current_term)

		rf.persistStateAndSnapshot(snapshot)

	}
}

/****** RPC-related Structures ******/
type RequestVoteArgs struct {
	CurrentTerm  int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	CurrentTerm  int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	CurrentTerm        int
	FirstConflictIndex int
	Success            bool
}

type InstallSnapshotArgs struct {
	CurrentTerm       int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	CurrentTerm int
}

/****** RPC-related Structures ******/

/*
 @brief RequestVote RPC handler.
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CurrentTerm > rf.current_term {
		rf.updateTerm(args.CurrentTerm)
	}

	if rf.current_term == args.CurrentTerm && rf.state == 0 && (rf.voted_for == -1 || rf.voted_for == args.CandidateId) {
		// Election restriction
		var last_log_index, last_log_term int
		if len(rf.log) == 0 {
			last_log_index, last_log_term = rf.last_included_index, rf.last_included_term
		} else {
			last_log_index, last_log_term = rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
		}

		if args.LastLogTerm == last_log_term && args.LastLogIndex >= last_log_index {
			reply.VoteGranted = true
			rf.voted_for = args.CandidateId
			select {
			case rf.heartbeat_channel <- 1:
			default:
			}
			// Persist
			rf.persist()
			log.Printf("Follower %v votes for Candidate %v... (Current term: %v)", rf.me, args.CandidateId, rf.current_term)
		} else if args.LastLogTerm != last_log_term && args.LastLogTerm > last_log_term {
			reply.VoteGranted = true
			rf.voted_for = args.CandidateId
			select {
			case rf.heartbeat_channel <- 1:
			default:
			}
			//Persist
			rf.persist()
			log.Printf("Follower %v votes for Candidate %v... (Current term: %v)", rf.me, args.CandidateId, rf.current_term)
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	reply.CurrentTerm = rf.current_term
}

/*
 @brief AppendEntries RPC handler
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update current term if seeing larger term
	if args.CurrentTerm > rf.current_term {
		rf.updateTerm(args.CurrentTerm)
		reply.CurrentTerm = args.CurrentTerm
	} else if args.CurrentTerm == rf.current_term {
		reply.CurrentTerm = rf.current_term
	} else if args.CurrentTerm < rf.current_term { // Leader is out-dated -> Return false
		reply.CurrentTerm = rf.current_term
		reply.Success = false
		return
	}

	select {
	case rf.heartbeat_channel <- 1:
	default:
	}

	// Consistency check
	var consistent bool
	prev_log_index := args.PrevLogIndex
	prev_log_term := args.PrevLogTerm

	if prev_log_index-rf.last_included_index <= 0 { // Leader sends all its entries -> Consistent
		consistent = true
	} else if rf.last_included_index+len(rf.log) < prev_log_index { // No entry at prevLogIndex -> Inconsistent
		consistent = false
	} else if rf.log[prev_log_index-rf.last_included_index-1].Term != prev_log_term { // Terms conflict at prevLogIndex -> Inconsistent
		consistent = false
	} else {
		consistent = true
	}

	// Handling heartbeat or appending entries
	if len(args.Entries) == 0 { // Heartbeat
		if rf.state == 1 {
			rf.state = 0
		}
	} else { // Append entries
		if consistent {
			// Skip those exsited log entries
			var index_log int
			var index_entries int
			if prev_log_index <= rf.last_included_index {
				index_log = 0
				index_entries = rf.last_included_index - prev_log_index
			} else {
				index_log = prev_log_index - rf.last_included_index
				index_entries = 0
			}

			// index_log := prev_log_index - rf.last_included_index
			// index_entries := 0

			for index_log < len(rf.log) && index_entries < len(args.Entries) && rf.log[index_log].Term == args.Entries[index_entries].Term {
				index_log += 1
				index_entries += 1
			}

			if index_entries < len(args.Entries) {
				rf.log = append(rf.log[0:index_log], args.Entries[index_entries:len(args.Entries)]...)
			}

			reply.Success = true

			// for debug
			rf.printLogInfo()

			// Persist
			rf.persist()
		} else {
			// Get first conflict index
			if rf.last_included_index+len(rf.log) < prev_log_index {
				reply.FirstConflictIndex = rf.last_included_index + len(rf.log) + 1
				log.Printf("Server %v has no entry at prevLogIndex -> FirstConflictIndex = %v... (Current term: %v)", rf.me, reply.FirstConflictIndex, rf.current_term)
			} else if rf.log[prev_log_index-rf.last_included_index-1].Term != prev_log_term {
				conflict_term := rf.log[prev_log_index-rf.last_included_index-1].Term
				for reply.FirstConflictIndex = prev_log_index; ; {
					if reply.FirstConflictIndex-rf.last_included_index >= 2 && rf.log[reply.FirstConflictIndex-rf.last_included_index-2].Term == conflict_term {
						reply.FirstConflictIndex -= 1
					} else {
						break
					}
				}
				log.Printf("Server %v has conflict term at prevLogIndex -> FirstConflictIndex = %v... (Current term: %v)", rf.me, reply.FirstConflictIndex, rf.current_term)
			}

			reply.Success = false
		}
	}

	// Update committed log entries
	if consistent && args.LeaderCommit > rf.commit_index && prev_log_index+len(args.Entries) > rf.commit_index {
		old_commit_index := rf.commit_index
		if args.LeaderCommit <= prev_log_index+len(args.Entries) {
			rf.commit_index = args.LeaderCommit
		} else {
			rf.commit_index = prev_log_index + len(args.Entries)
		}
		log.Printf("Follower %v's commitIndex is updated from %v to %v... (Current term: %v)", rf.me, old_commit_index, rf.commit_index, rf.current_term)
	}
}

/*
 @brief InstallSnapshot RPC handler
*/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update current term if seeing larger term
	if args.CurrentTerm > rf.current_term {
		rf.updateTerm(args.CurrentTerm)
		reply.CurrentTerm = args.CurrentTerm
	} else if args.CurrentTerm == rf.current_term {
		reply.CurrentTerm = rf.current_term
	} else if args.CurrentTerm < rf.current_term { // Leader is out-dated -> Return
		reply.CurrentTerm = rf.current_term
		return
	}

	if args.LastIncludedIndex <= rf.last_included_index { // Snapshot is out-dated -> Not send to service
		log.Printf("Server %v sees out-dated snapshot: lastIncludedIndex = %v, lastApplied = %v... (Current term: %v)",
			rf.me, args.LastIncludedIndex, rf.last_applied, rf.current_term)
		return
	} else if args.LastIncludedIndex >= rf.last_included_index+len(rf.log) ||
		rf.log[args.LastIncludedIndex-rf.last_included_index-1].Term != args.LastIncludedTerm { // Snapshot conflicts with log -> Discard entire log
		rf.last_included_index = args.LastIncludedIndex
		rf.last_included_term = args.LastIncludedTerm
		rf.log = []LogEntry{}
		log.Printf("Server %v installs snapshot: lastIncludedIndex = %v, lastIncludedTerm = %v... (Current term: %v)",
			rf.me, args.LastIncludedIndex, args.LastIncludedTerm, rf.current_term)
		rf.persistStateAndSnapshot(args.Data)
	} else {
		new_log := []LogEntry{}
		new_log = append(new_log, rf.log[args.LastIncludedIndex-rf.last_included_index:len(rf.log)]...)
		rf.log = new_log
		rf.last_included_index = args.LastIncludedIndex
		rf.last_included_term = args.LastIncludedTerm
		log.Printf("Server %v installs snapshot: lastIncludedIndex = %v, lastIncludedTerm = %v... (Current term: %v)",
			rf.me, args.LastIncludedIndex, args.LastIncludedTerm, rf.current_term)
		rf.persistStateAndSnapshot(args.Data)
	}
	rf.commit_index = rf.last_included_index

	// Send snapshot to service
	// msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data,
	// 	SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
	// rf.applyCh <- msg
	// rf.last_applied = rf.last_included_index
	// rf.commit_index = rf.last_included_index
	// log.Printf("Server %v sends snapshot to service: lastIncludedIndex = %v, lastIncludedTerm = %v... (Current term: %v)",
	// 	rf.me, args.LastIncludedIndex, args.LastIncludedTerm, rf.current_term)
}

/*
 @brief: Send a RequestVote RPC to a server.
*/
func (rf *Raft) sendRequestVote(server int) {
	var last_log_term, last_log_index int
	rf.mu.Lock()
	current_term := rf.current_term
	candidate_id := rf.me
	if len(rf.log) == 0 {
		last_log_term, last_log_index = rf.last_included_term, rf.last_included_index
	} else {
		last_log_term, last_log_index = rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Index
	}
	rf.mu.Unlock()

	log.Printf("Candidate %v is sending vote request to server %v... (Current term: %v)", candidate_id, server, current_term)

	args := RequestVoteArgs{CurrentTerm: current_term, CandidateId: candidate_id, LastLogTerm: last_log_term, LastLogIndex: last_log_index}
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	rf.mu.Lock()
	if ok {
		server_term := reply.CurrentTerm
		vote_granted := reply.VoteGranted
		if server_term > rf.current_term { // Candidate out of date
			log.Printf("Candidate %v's term %v is out of date (newer term %v), reverting to follower...", candidate_id, rf.current_term, server_term)
			rf.updateTerm(server_term)
		} else if server_term == rf.current_term && vote_granted {
			log.Printf("Candidate %v received a vote from server %v... (Current term: %v)", candidate_id, server, rf.current_term)
			rf.vote_count += 1

			// Inform the main goroutine if getting majority vote
			if rf.vote_count > len(rf.peers)/2 {
				select {
				case rf.vote_channel <- 1:
				default:
				}
			}
		}
	} else {
		log.Printf("Candidate %v fails to send vote request to server %v! (Current term: %v)", candidate_id, server, current_term)
	}
	rf.mu.Unlock()
}

/*
 @brief: Send a hearbeat (empty AppendEntries RPC) to a server
*/
func (rf *Raft) sendHeartBeat(server int, current_term int, next_index int,
	last_included_index int, last_included_term int, log_copy []LogEntry) {
	rf.mu.Lock()
	id := rf.me
	prev_log_index := next_index - 1
	var prev_log_term int
	if prev_log_index <= last_included_index {
		prev_log_term = last_included_term
	} else {
		log_offset := prev_log_index - last_included_index - 1
		prev_log_term = log_copy[log_offset].Term
	}
	leader_commit := rf.commit_index
	rf.mu.Unlock()

	log.Printf("Leader %v is sending heartbeat to server %v... (Current term: %v)", id, server, current_term)
	args := AppendEntriesArgs{CurrentTerm: current_term, LeaderId: id,
		PrevLogIndex: prev_log_index, PrevLogTerm: prev_log_term,
		Entries: []LogEntry{}, LeaderCommit: leader_commit}
	reply := AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	if ok {
		if reply.CurrentTerm > current_term { // This leader is out-dated
			log.Printf("Leader %v's term %v is out of date (newer term %v), reverting to follower...", id, current_term, reply.CurrentTerm)
			rf.updateTerm(reply.CurrentTerm)
		}
	} else {
		log.Printf("Leader %v fails to send heartbeat to server %v! (Current term: %v)", id, server, current_term)
	}
	rf.mu.Unlock()
}

/*
 @brief: Send log entries to a server
*/
func (rf *Raft) sendLogEntries(server int, current_term int, next_index int,
	last_included_index int, last_included_term int, log_copy []LogEntry) {
	rf.mu.Lock()
	id := rf.me
	log_len := len(log_copy)
	prev_log_index := next_index - 1
	var prev_log_term int
	if prev_log_index <= last_included_index {
		prev_log_term = last_included_term
	} else {
		log_offset := prev_log_index - last_included_index - 1
		prev_log_term = log_copy[log_offset].Term
	}
	leader_commit := rf.commit_index
	start_offset := next_index - last_included_index - 1
	var entries []LogEntry
	entries = append(entries, log_copy[start_offset:len(log_copy)]...)
	rf.mu.Unlock()

	log.Printf("Leader %v is sending log entries (Index starting from %v) to server %v... (Current term: %v)", id, prev_log_index+1, server, current_term)
	args := AppendEntriesArgs{CurrentTerm: current_term, LeaderId: id,
		PrevLogIndex: prev_log_index, PrevLogTerm: prev_log_term,
		Entries: entries, LeaderCommit: leader_commit}
	reply := AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	if ok {
		if reply.CurrentTerm > current_term { // This leader is out-dated
			log.Printf("Leader %v's term %v is out of date (newer term %v), reverting to follower...", id, current_term, reply.CurrentTerm)
			rf.updateTerm(reply.CurrentTerm)
		} else if reply.Success == false { // Inconsistency -> Update nextIndex
			rf.next_index[server] = reply.FirstConflictIndex
			log.Printf("Leader %v's log is inconsistent with server %v! First conflict index is %v... (Current term: %v)", id, server, reply.FirstConflictIndex, current_term)
		} else { // Success
			rf.next_index[server] = last_included_index + log_len + 1
			rf.match_index[server] = last_included_index + log_len
			log.Printf("Leader %v succeeded to append log entries to server %v! (Current term: %v)", id, server, current_term)
		}
	} else {
		log.Printf("Leader %v fails to send AppendEntries RPC to server %v! (Current term: %v)", id, server, current_term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendSnapshot(server int, current_term int, last_included_index int, last_included_term int, data []byte) {
	rf.mu.Lock()
	id := rf.me
	rf.mu.Unlock()

	log.Printf("Leader %v is sending snapshot (lastIncludedIndex = %v, lastIncludedTerm = %v) to server %v... (Current term: %v)",
		id, last_included_index, last_included_term, server, current_term)
	args := InstallSnapshotArgs{CurrentTerm: current_term, LeaderId: id,
		LastIncludedIndex: last_included_index, LastIncludedTerm: last_included_term, Data: data}
	reply := InstallSnapshotReply{}

	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	if ok {
		if reply.CurrentTerm > current_term {
			log.Printf("Leader %v's term %v is out of date (newer term %v), reverting to follower...", id, current_term, reply.CurrentTerm)
			rf.updateTerm(reply.CurrentTerm)
		} else {
			rf.next_index[server] = last_included_index + 1
			rf.match_index[server] = last_included_index
		}
	} else {
		log.Printf("Leader %v fails to send InstallSnapshot RPC to server %v! (Current term: %v)", id, server, current_term)
	}
	rf.mu.Unlock()
}

//
// Update current term when seeing larger term
// Leader or candidate will revert to follower
// voted_for will be set to -1
//
func (rf *Raft) updateTerm(new_term int) {
	if new_term > rf.current_term {
		rf.current_term = new_term
		rf.state = 0
		rf.voted_for = -1

		// Persist
		rf.persist()
	}
}

/*
 @brief Append log entry with given command into local log
 @return Index of newly appended log entry
*/
func (rf *Raft) appendEntryLocal(command interface{}) int {
	rf.mu.Lock()
	index := rf.last_included_index + len(rf.log) + 1
	log_entry := LogEntry{Index: index, Term: rf.current_term, Command: command}
	rf.log = append(rf.log, log_entry)

	// For debug
	rf.printLogInfo()

	// Persist
	rf.persist()
	rf.mu.Unlock()

	return index
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
	index := 0
	term, isLeader := rf.GetState()

	if isLeader {
		index = rf.appendEntryLocal(command)
		log.Printf("Command %v has been appended in leader %v... (Current term: %v)", command, rf.me, term)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
 @brief: The ticker is responsible for running server duties
*/
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		server_state := rf.state
		rf.mu.Unlock()

		if server_state == 2 { // Leader
			rf.leaderRoutine()
		} else if server_state == 0 { // Follower
			rf.followerRoutine()
		} else if server_state == 1 { // Candidate
			rf.candidateRoutine()
		}
	}
}

/*
 @brief: Leader's routine:
 		 Sending out hearbeat;
		 Sending log entries;
		 Commit log entries;
		 Apply log entries;
*/
func (rf *Raft) leaderRoutine() {
	rf.mu.Lock()
	me := rf.me
	num_server := len(rf.peers)
	current_term := rf.current_term
	rf.mu.Unlock()

	// Leader sending heartbeat
	for i := 0; i < num_server; i++ {
		if i != me {
			rf.mu.Lock()
			next_index := rf.next_index[i]
			last_included_index := rf.last_included_index
			last_included_term := rf.last_included_term
			log_copy := []LogEntry{}
			log_copy = append(log_copy, rf.log...)
			rf.mu.Unlock()

			go rf.sendHeartBeat(i, current_term, next_index, last_included_index, last_included_term, log_copy)
		}
	}

	for i := 0; i < 10; i++ {
		// Leader sending log entries
		for i := 0; i < num_server; i++ {
			if i != me {
				rf.mu.Lock()
				next_index := rf.next_index[i]
				last_log_index := rf.last_included_index + len(rf.log)
				last_included_index := rf.last_included_index
				last_included_term := rf.last_included_term
				log_copy := []LogEntry{}
				log_copy = append(log_copy, rf.log...)
				data := rf.persister.ReadSnapshot()
				rf.mu.Unlock()

				if last_log_index >= next_index {
					// go rf.sendLogEntries(i, current_term, next_index, last_included_index, last_included_term, log_copy)
					if next_index > last_included_index {
						go rf.sendLogEntries(i, current_term, next_index, last_included_index, last_included_term, log_copy)
					} else {
						go rf.sendSnapshot(i, current_term, last_included_index, last_included_term, data)
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		// Commit those uncommitted log entries
		for index := rf.commit_index + 1; index <= rf.last_included_index+len(rf.log); index++ {
			log_offset := index - rf.last_included_index - 1
			if rf.log[log_offset].Term != current_term {
				continue
			}
			cnt := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.match_index[i] >= index {
					cnt += 1
				}
			}
			if cnt > len(rf.peers)/2 {
				old_commit_index := rf.commit_index
				rf.commit_index = index
				log.Printf("Leader %v's commitIndex is updated from %v to %v... (Current term: %v)", rf.me, old_commit_index, rf.commit_index, current_term)
			} else {
				break
			}
		}

		// Apply those unapplied command
		apply_success := true
		for index := rf.last_applied + 1; index > rf.last_included_index && index <= rf.commit_index; index++ {
			log_offset := index - rf.last_included_index - 1
			apply_msg := ApplyMsg{CommandValid: true, Command: rf.log[log_offset].Command, CommandIndex: index}
			select {
			case rf.applyCh <- apply_msg:
				rf.last_applied = index
				log.Printf("Command %v has been applied in leader %v!", index, rf.me)
			case <-time.After(10 * time.Millisecond):
				log.Printf("Fail to apply command %v in 10 ms in server %v!", index, rf.me)
				apply_success = false
			}

			if !apply_success {
				break
			}
		}
		rf.mu.Unlock()
	}
}

/*
 @brief: Follower's routine: Checking recent hearbeat
*/
func (rf *Raft) followerRoutine() {
	rf.mu.Lock()
	rf.recent_heartbeat = false
	election_timeout := rf.election_timeout
	rf.mu.Unlock()

	select {
	case <-rf.heartbeat_channel:
		// Re-randomize election timeout
		rf.mu.Lock()
		rand.Seed(time.Now().UnixNano())
		rf.election_timeout = rand.Intn(election_timeout_up-election_timeout_lb) + election_timeout_lb
		rf.mu.Unlock()
	case <-time.After(time.Duration(election_timeout) * time.Millisecond):
		// Timeout: Convert to Candidate
		rf.mu.Lock()
		log.Printf("Follower %v fails to receive recent heartbeat, converting to Candidate... (Current term: %v)", rf.me, rf.current_term)
		rf.state = 1
		rf.mu.Unlock()
	}

	// Apply those applied command
	rf.mu.Lock()
	if rf.last_included_index > rf.last_applied { // Apply snapshot
		snapshot_data := rf.persister.ReadSnapshot()
		apply_msg := ApplyMsg{SnapshotValid: true, Snapshot: snapshot_data,
			SnapshotIndex: rf.last_included_index, SnapshotTerm: rf.last_included_term}
		select {
		case rf.applyCh <- apply_msg:
			rf.last_applied = rf.last_included_index
			rf.commit_index = rf.last_included_index
			log.Printf("Server %v applies snapshot: lastIncludedIndex = %v, lastIncludedTerm = %v... (Current term: %v)",
				rf.me, rf.last_included_index, rf.last_included_term, rf.current_term)
		case <-time.After(10 * time.Millisecond):
			log.Printf("Fail to apply snapshot in 10 ms in server %v: lastIncludedIndex = %v, lastIncludedTerm = %v... (Current term: %v)",
				rf.me, rf.last_included_index, rf.last_included_term, rf.current_term)
		}
	} else { // Apply log entries
		apply_success := true
		for index := rf.last_applied + 1; index <= rf.commit_index; index++ {
			log_offset := index - rf.last_included_index - 1
			apply_msg := ApplyMsg{CommandValid: true, Command: rf.log[log_offset].Command, CommandIndex: index}
			select {
			case rf.applyCh <- apply_msg:
				rf.last_applied = index
				log.Printf("Command %v has been applied in server %v!", index, rf.me)
			case <-time.After(10 * time.Millisecond):
				log.Printf("Fail to apply command %v in 10 ms in server %v!", index, rf.me)
				apply_success = false
			}

			if !apply_success {
				break
			}
		}
	}
	rf.mu.Unlock()
}

/*
 @brief: Candidate's routine: Trying to obtain votes until leader emerges
*/
func (rf *Raft) candidateRoutine() {
	for {
		rf.mu.Lock()
		me := rf.me
		// Increment term
		rf.current_term += 1
		// Vote for self
		rf.vote_count = 1
		rf.voted_for = rf.me
		//Persist
		rf.persist()
		// Reset election timeout
		rand.Seed(time.Now().UnixNano())
		rf.election_timeout = rand.Intn(election_timeout_up-election_timeout_lb) + election_timeout_lb
		election_timeout := rf.election_timeout
		rf.mu.Unlock()

		// Send vote request
		for i := 0; i < len(rf.peers); i++ {
			if i != me {
				go rf.sendRequestVote(i)
			}
		}

		// Wait for majority vote
		majority_vote := false
		select {
		case <-rf.vote_channel:
			majority_vote = true
		case <-time.After(time.Duration(election_timeout) * time.Millisecond):
		}

		rf.mu.Lock()
		if rf.state != 1 {
			rf.mu.Unlock()
			break
		}

		if majority_vote {
			log.Printf("Candidate %v obtained majority vote, now becomes leader... (Current term: %v)", rf.me, rf.current_term)
			rf.state = 2
			rf.next_index = make([]int, len(rf.peers))
			rf.match_index = make([]int, len(rf.peers))
			// Initialize nextIndex for leader
			for i := 0; i < len(rf.next_index); i++ {
				rf.next_index[i] = rf.last_included_index + len(rf.log) + 1
				rf.match_index[i] = -1
			}
			rf.mu.Unlock()
			break
		} else {
			log.Printf("Candidate %v sees split vote, starting new election... (Current term: %v)", rf.me, rf.current_term)
		}
		rf.mu.Unlock()
	}
}

/*
	For debug
*/
func (rf *Raft) printLogInfo() {
	log.Printf("LogInfo of server %v:", rf.me)
	log.Printf("Current log: %v", rf.log)
	log.Printf("Current commitIndex = %v", rf.commit_index)
	log.Printf("Current lastApplied = %v", rf.last_applied)
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
	rf.heartbeat_channel = make(chan int, 1)
	rf.vote_channel = make(chan int)
	rf.applyCh = applyCh
	rf.current_term = 0
	rf.state = 0
	rf.commit_index = 0
	rf.last_applied = 0
	rf.last_included_index = 0
	rf.last_included_term = 0
	rand.Seed(time.Now().UnixNano())
	rf.election_timeout = rand.Intn(election_timeout_up-election_timeout_lb) + election_timeout_lb

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to run this server
	go rf.ticker()

	return rf
}
