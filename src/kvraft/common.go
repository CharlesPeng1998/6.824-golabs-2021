package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

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
