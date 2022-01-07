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
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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

const election_timeout_lb int = 500
const election_timeout_up int = 800

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	current_term     int
	state            int // 0 for follower, 1 for candidate, 2 for leader
	election_timeout int
	recent_heartbeat bool
	vote_count       int
	log              []LogEntry
}

type LogEntry struct {
	// TODO
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.current_term
	isleader = (rf.state == 2)
	rf.mu.Unlock()

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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
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
	Current_term int
	CandidateID  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Current_term int
	Vote_granted bool
}

type AppendEntriesArgs struct {
	Current_term int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Current_term int
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if rf.state != 0 {
		return
	}
	if args.Current_term >= rf.current_term {
		rf.current_term = args.Current_term
		reply.Vote_granted = true
	} else {
		reply.Vote_granted = false
	}
	reply.Current_term = rf.current_term
	rf.mu.Unlock()
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 { // Heartbeat
		rf.mu.Lock()
		if args.Current_term > rf.current_term { // See larger term
			rf.current_term = args.Current_term
			rf.state = 0
		}
		if rf.state == 1 { // Candidate reverts to Follower
			rf.state = 0
		}

		reply.Current_term = rf.current_term
		rf.mu.Unlock()
	}

	// TODO...
}

//
// Send a RequestVote RPC to a server.
//
func (rf *Raft) sendRequestVote(server int) {
	rf.mu.Lock()
	candidate_term := rf.current_term
	candidate_id := rf.me
	rf.mu.Unlock()

	log.Printf("Candidate %v is sending vote request to server %v...", candidate_id, server)

	args := RequestVoteArgs{Current_term: candidate_term, CandidateID: candidate_id}
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		server_term := reply.Current_term
		vote_granted := reply.Vote_granted
		rf.mu.Lock()
		if server_term > rf.current_term { // Candidate out of date
			log.Printf("Candidate %v's term %v is out of date (newer term %v), reverting to follower...", candidate_id, rf.current_term, server_term)
			rf.state = 0
			rf.current_term = server_term
		} else if vote_granted {
			rf.vote_count += 1
		}
		rf.mu.Unlock()
	} else {
		log.Printf("Candidate %v fails to send vote request to server %v!", candidate_id, server)
	}
}

//
// Send a hearbeat (empty AppendEntries RPC) to a server
//
func (rf *Raft) sendHeartBeat(server int) {
	rf.mu.Lock()
	id := rf.me
	current_term := rf.current_term
	rf.mu.Unlock()

	log.Printf("Leader %v is sending heartbeat to server %v...", id, server)
	args := AppendEntriesArgs{Current_term: current_term, Entries: []LogEntry{}}
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if ok {
		server_term := reply.Current_term
		rf.mu.Lock()
		if server_term > rf.current_term {
			log.Printf("Leader %v's term %v is out of date (newer term %v), reverting to follower...", id, rf.current_term, server_term)
			rf.current_term = server_term
			rf.state = 0
		}
		rf.mu.Unlock()
	} else {
		log.Printf("Leader %v fails to contact server %v!", id, server)
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		server_state := rf.state
		num_server := len(rf.peers)
		id := rf.me
		rf.mu.Unlock()

		if server_state == 2 { // Leader
			for i := 0; i < num_server; i++ {
				if i != id {
					go rf.sendHeartBeat(i)
				}
			}
			time.Sleep(100 * time.Millisecond)
		} else if server_state == 0 { // Follower
			rf.mu.Lock()
			rf.recent_heartbeat = false
			election_timeout := rf.election_timeout
			rf.mu.Unlock()

			// Sleep for an election timeout
			time.Sleep(time.Duration(election_timeout) * time.Millisecond)

			rf.mu.Lock()
			if rf.recent_heartbeat {
				// Re-randomize election timeout
				rand.Seed(time.Now().UnixNano())
				rf.election_timeout = rand.Intn(election_timeout_up-election_timeout_lb) + election_timeout_lb
			} else {
				log.Printf("Follower %v fails to receive recent heartbeat, converting to Candidate...", rf.me)
				// Convert to Candidate
				rf.state = 1
			}
			rf.mu.Unlock()
		} else if server_state == 1 { // Candidate
			for {
				rf.mu.Lock()
				log.Printf("Candidate %v is starting election...", rf.me)
				// Increment term
				rf.current_term += 1
				// Vote for self
				rf.vote_count = 1
				// Reset election timeout
				rand.Seed(time.Now().UnixNano())
				rf.election_timeout = rand.Intn(election_timeout_up-election_timeout_lb) + election_timeout_lb
				election_timeout := rf.election_timeout
				rf.mu.Unlock()

				// Send vote request
				for i := 0; i < len(rf.peers); i++ {
					if i != id {
						go rf.sendRequestVote(i)
					}
				}

				// Sleep for an election timeout
				time.Sleep(time.Duration(election_timeout) * time.Millisecond)

				rf.mu.Lock()
				if rf.state != 1 {
					break
				}
				if rf.vote_count > num_server/2 {
					log.Printf("Candidate %v obtained majority vote, now becomes leader...", rf.me)
					rf.state = 2
					break
				} else {
					log.Printf("Candidate %v sees split vote, starting new election...", rf.me)
				}
				rf.mu.Unlock()
			}
		}
	}
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
	rf.current_term = 0
	rf.state = 0
	rand.Seed(time.Now().UnixNano())
	rf.election_timeout = rand.Intn(election_timeout_up-election_timeout_lb) + election_timeout_lb

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
