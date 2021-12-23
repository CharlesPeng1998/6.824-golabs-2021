package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := MapReduceArgs{Message: "task_request"}
	reply := MapReduceReply{}

	for {
		connected := TaskRequest(&args, &reply)

		// Connection fails or receive work exit signal
		if !connected || reply.Type == 3 {
			break
		}

		// Worker stand by
		if reply.Type == 2 {
			time.Sleep(time.Second)
		}

		// TODO: Map tasks
		if reply.Type == 0 {
			num_reduce := reply.Num_reduce
			filename := reply.Message
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Cannot read %v", filename)
			}
			file.Close()
			kv_list := mapf(filename, string(content))

			// Partitioning
			kv_partition_list := make([][]KeyValue, num_reduce)
			for _, kv := range kv_list {
				id_reduce := ihash(kv.Key) % num_reduce
				kv_partition_list[id_reduce] = append(kv_partition_list[id_reduce], kv)
			}

			for i := 0; i < num_reduce; i++ {
				//
			}
		}

		// TODO: Reduce tasks
	}

	return
}

func TaskRequest(args *MapReduceArgs, reply *MapReduceReply) bool {
	return call("Coordinator.TaskDistribution", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)

	if err != nil {
		log.Printf("Connection failed!")
		return false
	}

	defer c.Close()
	err = c.Call(rpcname, args, reply)

	return true
}
