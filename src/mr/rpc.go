package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskRequestArgs struct {
	// TODO
}

type TaskRequestReply struct {
	Id_map_task    int
	Id_reduce_task int
	Num_reduce     int

	// 0 for map, 1 for reduce, 2 for stand by, 3 for work exit
	Type    int
	Message string
}

type TaskFinishArgs struct {
	Id_map_task    int
	Id_reduce_task int
	Type           int // 0 for map, 1 for reduce
}

type TaskFinishReply struct {
	Ack bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
