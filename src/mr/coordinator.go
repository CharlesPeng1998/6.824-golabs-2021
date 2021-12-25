package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	input_files []string
	// task states: 0 for unassigned, 1 for assigned, 2 for finished
	map_task_states    []int
	reduce_task_states []int
	num_reduce         int
	num_map            int
	mux                sync.Mutex
}

// RPC handler assigning task to worker
func (c *Coordinator) TaskRequestHandler(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mux.Lock()
	id_map := -1
	for i := 0; i < c.num_map; i++ {
		if c.map_task_states[i] == 0 {
			id_map = i
			break
		}
	}
	if id_map == -1 {
		// TODO: This is a temporary setting
		// without considering the reduce tasks
		reply.Type = 3
	} else {
		reply.Type = 0
		reply.Id_map_task = id_map
		reply.Num_reduce = c.num_reduce
		reply.Message = c.input_files[id_map]
		c.map_task_states[id_map] = 1
	}
	c.mux.Unlock()

	// Start a goroutine to iteratively check task state
	go c.CheckMapTaskState(id_map)

	return nil
}

// RPC handler dealing with task finish signal from worker
func (c *Coordinator) TaskFinishHandler(args *TaskFinishArgs, reply *TaskFinishReply) error {
	c.mux.Lock()
	if args.Type == 0 {
		c.map_task_states[args.Id_map_task] = 2
	} else if args.Type == 1 {
		c.reduce_task_states[args.Id_reduce_task] = 2
	}
	reply.Ack = true
	return nil
}

// Check map task state.
// Running on a single goroutine.
// An unfinished task after 10 seconds will be unassigned again.
func (c *Coordinator) CheckMapTaskState(id_map int) {
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		c.mux.Lock()
		state := c.map_task_states[id_map]
		c.mux.Unlock()
		if state == 2 {
			return
		}
	}
	c.mux.Lock()
	c.map_task_states[id_map] = 0
	c.mux.Unlock()
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{input_files: files,
		map_task_states:    make([]int, len(files)),
		reduce_task_states: make([]int, nReduce),
		num_reduce:         nReduce,
		num_map:            len(files)}

	c.server()
	return &c
}
