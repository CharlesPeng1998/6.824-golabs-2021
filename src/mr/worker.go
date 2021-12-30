package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KVList []KeyValue

func (a KVList) Len() int           { return len(a) }
func (a KVList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KVList) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	task_request_args := TaskRequestArgs{}
	task_request_reply := TaskRequestReply{}

	for {
		connected := SendTaskRequestSignal(&task_request_args, &task_request_reply)

		if !connected {
			log.Printf("Fail to send task request signal!")
			time.Sleep(time.Second)
			continue
		}

		if task_request_reply.Type == 3 {
			log.Printf("Worker is told to exit!")
			break
		}

		// Worker stand by
		if task_request_reply.Type == 2 {
			log.Printf("No task assigned! Standing by...")
			time.Sleep(time.Second)
			continue
		}

		// Map tasks
		if task_request_reply.Type == 0 {
			id_map := task_request_reply.Id_map_task
			num_reduce := task_request_reply.Num_reduce
			filename := task_request_reply.Message

			log.Printf("Launching map task %v for input file %v", id_map, filename)

			// Reading input file
			file, err := os.Open(filename)
			if err != nil {
				log.Printf("Map task %v: Cannot open %v! Task is aborted!", id_map, filename)
				time.Sleep(time.Second)
				continue
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Printf("Map task %v: Cannot read %v! Task is aborted", id_map, filename)
				file.Close()
				time.Sleep(time.Second)
				continue
			}
			file.Close()

			// Running user-defined map function
			kv_list := mapf(filename, string(content))

			// Partitioning
			kv_partition_list := make([][]KeyValue, num_reduce)
			for _, kv := range kv_list {
				id_reduce := ihash(kv.Key) % num_reduce
				kv_partition_list[id_reduce] = append(kv_partition_list[id_reduce], kv)
			}

			// Write intermediate files
			written := true
			for i := 0; i < num_reduce; i++ {
				sort.Sort(KVList(kv_partition_list[i]))
				written = WriteIntermediateFile(kv_partition_list[i], id_map, i)
				if !written {
					break
				}
			}
			if !written {
				log.Printf("Map task %v: Fail to write intermediate files! Task is aborted!", id_map)
				time.Sleep(time.Second)
				continue
			}
			log.Printf("Map task %v: Output has been written to files! Informing master...", id_map)

			// Inform master task has been finished
			task_finish_args := TaskFinishArgs{Id_map_task: id_map, Type: 0}
			task_finish_reply := TaskFinishReply{Ack: false}
			ret := SendTaskFinishSignal(&task_finish_args, &task_finish_reply)
			if !ret {
				log.Printf("Map task %v: Fail to send task finish signal to master! Task is aborted!", id_map)
				time.Sleep(time.Second)
				continue
			}

			if task_finish_reply.Ack {
				log.Printf("Map task %v: Task has been acknowledged by master!", id_map)
			} else {
				log.Printf("Map task %v: Task is not acknowledged by master!", id_map)
			}
		}

		// TODO: Reduce tasks
	}
	return
}

func SendTaskRequestSignal(args *TaskRequestArgs, reply *TaskRequestReply) bool {
	return call("Coordinator.TaskRequestHandler", &args, &reply)
}

func SendTaskFinishSignal(args *TaskFinishArgs, reply *TaskFinishReply) bool {
	return call("Coordinator.TaskFinishHandler", &args, &reply)
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

func WriteIntermediateFile(kv_list []KeyValue, id_map int, id_reduce int) bool {
	intermediate_filename := fmt.Sprintf("mr-%v-%v", id_map, id_reduce)
	file, err := ioutil.TempFile("./", intermediate_filename)
	if err != nil {
		log.Printf("Map task %v: Fail to create intermediate file: %v!", id_map, intermediate_filename)
		return false
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(kv_list)

	if err != nil {
		log.Printf("Map task %v: Fail to encode json in file: %v", id_map, intermediate_filename)
		return false
	}

	os.Rename(file.Name(), intermediate_filename)
	return true
}
