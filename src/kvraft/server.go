package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Clerk_id int64
	Op_id    int
	Type     int // 0 for Put, 1 for Append and 2 for Get
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate  int               // snapshot if log grows this big
	clerk2maxOpId map[int64]int     // the maximum operation id for each clerk
	informCh      map[int]chan Op   // channels used to inform RPC handlers
	kv_data       map[string]string // the key-value data
}

type GetArgs struct {
	Clerk_id int64
	Op_id    int
	Key      string
}

type GetReply struct {
	Success bool
	Value   string
}

type PutAppendArgs struct {
	Clerk_id int64
	Op_id    int
	Type     int // 0 for Put and 1 for Append
	Key      string
	Value    string
}

type PutAppendReply struct {
	Success bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	operation := Op{Clerk_id: args.Clerk_id, Op_id: args.Op_id, Type: 2, Key: args.Key}
	index, _, isLeader := kv.rf.Start(operation)

	if !isLeader { // This server is not leader
		reply.Success = false
		return
	}

	kv.mu.Lock()
	kv.informCh[index] = make(chan Op, 1)
	kv.mu.Unlock()

	select {
	case recv_operation := <-kv.informCh[index]:
		if recv_operation.Clerk_id == args.Clerk_id && recv_operation.Op_id == args.Op_id {
			log.Printf("KVServer %v's Get handler succeeds to see operation from clerk %v: Operation ID = %v, Key = %v, Value = %v",
				kv.me, recv_operation.Clerk_id, recv_operation.Op_id, recv_operation.Key, recv_operation.Value)
			reply.Success = true
			reply.Value = recv_operation.Value
		} else {
			log.Printf("KVServer %v' Get handler sees different operation at index %v", kv.me, index)
			reply.Success = false
		}
	case <-time.After(500 * time.Millisecond):
		log.Printf("KVServer %v's Get handler fails to see operation from clerk %v in 500ms: Operation ID = %v, Key = %v",
			kv.me, operation.Clerk_id, operation.Op_id, operation.Key)
	}

	kv.mu.Lock()
	delete(kv.informCh, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) apply() {
	for apply_msg := range kv.applyCh {
		operation := apply_msg.Command

		kv.mu.Lock()

		if kv.clerk2maxOpId[operation.Clerk_id] >= operation.Op_id && operation.Type == 0 && operation.Type == 1 { // Duplicate write operation
			log.Printf("KVServer %v sees duplicate write operation id from clerk %v: %v (Max operation ID = %v)",
				kv.me, operation.Clerk_id, operation.Op_id, kv.clerk2maxOpId[operation.Clerk_id])
		} else if operation.Type == 0 { // Put
			kv.kv_data[operation.Key] = operation.Value
			log.Printf("KVServer %v applies Put from clerk %v: Operation ID = %v, Key = %v, Value = %v",
				kv.me, operation.Clerk_id, operation.Op_id, operation.Key, operation.Value)
		} else if operation.Type == 1 { // Append
			kv.kv_data[operation.Key] += operation.Value
			log.Printf("KVServer %v applies Append from clerk %v: Operation ID = %v, Key = %v, Value = %v",
				kv.me, operation.Clerk_id, operation.Op_id, operation.Key, operation.Value)
		} else if operation.Type == 2 { // Get
			if value, ok := kv.kv_data[operation.Key]; ok {
				operation.Value = value
			} else {
				operation.Value = ""
			}
			log.Printf("KVServer %v applies Get from clerk %v: Operation ID = %v, Key = %v, Value = %v",
				kv.me, operation.Clerk_id, operation.Op_id, operation.Key, operation.Value)
		}

		kv.clerk2maxOpId[operation.Clerk_id] = operation.Op_id

		if kv.clerk2maxOpId[operation.Clerk_id] < operation.Op_id {
			kv.clerk2maxOpId[operation.Clerk_id] = operation.Op_id
		}

		index := apply_msg.CommandIndex
		if _, ok := kv.informCh[index]; ok {
			kv.informCh[index] <- operation
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.clerk2maxOpId = make(map[int64]int)
	kv.informCh = make(map[int]chan Op)
	kv.kv_data = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()

	return kv
}
