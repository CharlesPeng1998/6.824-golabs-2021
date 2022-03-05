package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	clerk_id     int64
	operation_id int
	last_leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerk_id = nrand()
	ck.operation_id = 0
	ck.last_leader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Clerk_id: ck.clerk_id, Op_id: ck.operation_id, Key: key}
	reply := GetReply{}

	possible_leader := ck.last_leader
	ret := ""

	for {
		log.Printf("Clerk %v is sending Get RPC to server %v: Key = %v, Operation ID = %v...",
			ck.clerk_id, possible_leader, key, ck.operation_id)
		ok := ck.servers[possible_leader].Call("KVServer.Get", &args, &reply)

		if ok && reply.Error == OK {
			log.Printf("Clerk %v succeeds in Get operation in server %v: Key = %v, Value = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, key, reply.Value, ck.operation_id)
			ret = reply.Value
			break
		} else if ok && reply.Error == ErrWrongLeader {
			log.Printf("Clerk %v sends Get RPC to the wrong leader %v: Key = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, key, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		} else if ok && reply.Error == ErrOpDiscarded {
			log.Printf("Clerk %v's Get operation to server %v is discarded: Key = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, key, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		} else if ok && reply.Error == ErrTimeout {
			log.Printf("Clerk %v's Get operation to server %v is timeout: Key = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, key, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		} else if !ok {
			log.Printf("Clerk %v fails to send Get RPC to server %v: Key = %v, Operation Id = %v...",
				ck.clerk_id, possible_leader, key, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		}
	}

	ck.operation_id += 1
	ck.last_leader = possible_leader
	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Clerk_id: ck.clerk_id, Op_id: ck.operation_id, Key: key, Value: value}
	if op == "Put" {
		args.Type = 0
	} else if op == "Append" {
		args.Type = 1
	}
	reply := PutAppendReply{}

	possible_leader := ck.last_leader

	for {
		log.Printf("Clerk %v is sending PutAppend RPC to server %v: Type = %v, Key = %v, Value = %v, Operation ID = %v...",
			ck.clerk_id, possible_leader, args.Type, key, value, ck.operation_id)
		ok := ck.servers[possible_leader].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Error == OK {
			log.Printf("Clerk %v succeeds in PutAppend operation in server %v: Type = %v, Key = %v, Value = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, args.Type, key, value, ck.operation_id)
			break
		} else if ok && reply.Error == ErrWrongLeader {
			log.Printf("Clerk %v sends PutAppend RPC to the wrong leader %v: Type = %v, Key = %v, Value = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, args.Type, key, value, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		} else if ok && reply.Error == ErrOpDiscarded {
			log.Printf("Clerk %v's PutAppend operation to server %v is discarded: Type = %v, Key = %v, Value = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, args.Type, key, value, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		} else if ok && reply.Error == ErrTimeout {
			log.Printf("Clerk %v's PutAppend operation to server %v is timeout: Type = %v, Key = %v, Value = %v, Operation ID = %v...",
				ck.clerk_id, possible_leader, args.Type, key, value, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		} else if !ok {
			log.Printf("Clerk %v fails to send PutAppend RPC to server %v: Type = %v, Key = %v, Value = %v, Operation Id = %v...",
				ck.clerk_id, possible_leader, args.Type, key, value, ck.operation_id)
			possible_leader = (possible_leader + 1) % len(ck.servers)
		}
	}

	ck.operation_id += 1
	ck.last_leader = possible_leader
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
