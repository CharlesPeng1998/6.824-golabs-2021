package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOpDiscarded = "ErrOpDiscarded"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type GetArgs struct {
	Clerk_id int64
	Op_id    int
	Key      string
}

type GetReply struct {
	Error Err
	Value string
}

type PutAppendArgs struct {
	Clerk_id int64
	Op_id    int
	Type     int // 0 for Put and 1 for Append
	Key      string
	Value    string
}

type PutAppendReply struct {
	Error Err
}
