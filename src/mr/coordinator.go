package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	input_files          []string
	map_task_finished    []bool
	reduce_task_finished []bool
	num_reduce           int
	num_map              int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskDistribution(args *MapReduceArgs, reply *MapReduceReply) error {
	// TODO
	fmt.Printf("MapReduceArgs message = %v\n", args.Message)
	reply.Id_map_task = 888
	reply.Id_reduce_task = 999
	reply.Num_reduce = c.num_reduce
	reply.Type = 0
	reply.Message = "Test"
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
		task_finished: make([]bool, len(files)),
		num_reduce:    nReduce,
		num_map:       len(files)}

	for i := range c.input_files {
		c.task_finished[i] = false
	}

	c.server()
	return &c
}
