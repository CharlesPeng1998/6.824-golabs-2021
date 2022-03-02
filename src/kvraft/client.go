package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
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
	args := GetArgs{ID: ck.operation_id, Key: key}
	reply := GetReply{}

	possible_leader := ck.last_leader
	ret := ""

	for {
		log.Printf("Clerk is sending Get RPC to server %v, Key = %v, operation ID = %v...",
			possible_leader, key, ck.operation_id)
		ok := ck.servers[possible_leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Success {
			log.Printf("Clerk succeeds in calling Get to server %v, Key = %v, Value = %v, operation ID = %v...",
				possible_leader, key, reply.Value, ck.operation_id)
			ret = reply.Value
			break
		} else {
			log.Printf("Clerk fails in calling Get to server %v, Key = %v, operation ID = %v! Resending...",
				possible_leader, key, ck.operation_id)
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
	args := PutAppendArgs{ID: ck.operation_id, Key: key, Value: value}
	if op == "Put" {
		args.Type = 0
	} else if op == "Append" {
		args.Type = 1
	}
	reply := PutAppendReply{}

	possible_leader := ck.last_leader

	for {
		log.Printf("Clerk is sending PutAppend RPC to server %v, Type = %v, Key = %v, Value = %v, operation ID = %v...",
			possible_leader, args.Type, key, value, ck.operation_id)
		ok := ck.servers[possible_leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Success {
			log.Printf("Clerk succeeds in calling PutAppend RPC to server %v, Type = %v, Key = %v, Value = %v, operation ID = %v...",
				possible_leader, args.Type, key, value, ck.operation_id)
			break
		} else {
			log.Printf("Clerk fails in calling PutAppend RPC to server %v, Type = %v, Key = %v, Value = %v, operation ID = %v! Resending...",
				possible_leader, args.Type, key, value, ck.operation_id)
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
