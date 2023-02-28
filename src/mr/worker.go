package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeFile(filename string, kvs []KeyValue) string {
	tmpFile, err := ioutil.TempFile("", "mr-tmp-*")
	if err != nil {
		log.Fatalf("create tmp file failed: %s", err.Error())
	}
	defer tmpFile.Close()

	enc := json.NewEncoder(tmpFile)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("encode file failed: %s", err.Error())
		}
	}

	dir, _ := os.Getwd()
	absPath := filepath.Join(dir, filename)
	os.Rename(tmpFile.Name(), absPath)
	return absPath
}

func readFile(filepath string) []KeyValue {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("read file failed: %s", err.Error())
	}

	kvs := make([]KeyValue, 0)
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func runMapTask(task *Task, mapf MapF) {
	content, err := os.ReadFile(task.File)
	if err != nil {
		log.Fatalf("read file failed: %s\n", err.Error())
	}

	kvs := mapf(task.File, string(content))
	buffer := make([][]KeyValue, task.NReduce)

	for _, kv := range kvs {
		reduceId := ihash(kv.Key) % task.NReduce
		buffer[reduceId] = append(buffer[reduceId], kv)
	}

	task.Intermediates = make([]string, task.NReduce)
	for reduceId, kvs := range buffer {
		filename := fmt.Sprintf("mr-%d-%d", task.ID, reduceId)
		absPath := writeFile(filename, kvs)
		task.Intermediates[reduceId] = absPath
	}
}

func runReduceTask(task *Task, reducef ReduceF) {
	kvs := make([]KeyValue, 0)
	for _, file := range task.Intermediates {
		kvs = append(kvs, readFile(file)...)
	}

	sort.Sort(ByKey(kvs))
	tmpFile, err := ioutil.TempFile("", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("create tmp file failed: %s", err.Error())
	}
	defer tmpFile.Close()

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	dir, _ := os.Getwd()
	filename := fmt.Sprintf("mr-out-%d", task.ID)
	absPath := filepath.Join(dir, filename)
	os.Rename(tmpFile.Name(), absPath)
}

// main/mrworker.go calls this function.
func Worker(mapf MapF, reducef ReduceF) {
	for {
		task := Task{}
		ok := call("Coordinator.AllocTask", &EmptyArgs{}, &task)
		if !ok {
			log.Fatalf("call Coordinator.AllocTask failed!\n")
		}

		log.Printf("start task: id %v, type %v", task.ID, task.Type)

		switch task.Type {
		case Map:
			runMapTask(&task, mapf)
		case Reduce:
			runReduceTask(&task, reducef)
		case Wait:
			time.Sleep(time.Second * 5)
			continue
		case Exit:
			return
		}

		ok = call("Coordinator.CompleteTask", &task, &EmptyReply{})
		if !ok {
			log.Fatalf("call Coordinator.CompleteTask failed!\n")
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
