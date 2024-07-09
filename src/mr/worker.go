package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	workerStatus := true
	for workerStatus {
		task := getTask()
		switch task.TaskStage {
		case Map:
			mapping(&task, mapf)
		case Reduce:
			reducing(&task,reducef)
		case Wait:
			time.Sleep(5*time.Second)
		case Exit:
			workerStatus=false		
		}
	}
}

func getTask() Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.AssignTask", &args, &reply)
	return reply.Task
}

func mapping(t *Task, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(t.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", t.FileName)
	}

	kvList := mapf(t.FileName, string(content))
	shelter := make([][]KeyValue, t.ReduceNum)
	for _, kv := range kvList {
		hash := ihash(kv.Key) % t.ReduceNum
		shelter[hash] = append(shelter[hash], kv)
	}

	mapOutFiles := make([]string, t.ReduceNum)
	for i, v := range shelter {
		mapOutFiles[i] = WriteToTempFile(t.TaskId, i, v)
	}

	args, reply := CompleteTaskArgs{
		TaskId:    t.TaskId,
		Stage:     t.TaskStage,
		FilePaths: mapOutFiles,
	}, CompleteTaskReply{}
	call("Coordinator.CompleteTask", &args, &reply)
}

func reducing(t *Task, reducef func(string, []string) string) {
	kvList := ReadFromTempFile(t.Intermediates)

	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].Key <= kvList[j].Key
	})

	val := make([]string, 0)
	rep := make([]KeyValue, 0)
	var last KeyValue
	if len(kvList) > 0 {
		last = kvList[0]
		for _, kv := range kvList {
			if kv.Key != last.Key {
				rep = append(rep, KeyValue{last.Key, reducef(last.Key, val)})
				last = kv
				val = make([]string, 0)
			}
			val = append(val, kv.Value)
		}
		if len(val) != 0 {
			rep = append(rep, KeyValue{last.Key, reducef(last.Key, val)})
		}
	}
	filePath := WriteToOutFile(t.TaskId, rep)

	args, reply := CompleteTaskArgs{
		TaskId:    t.TaskId,
		Stage:     t.TaskStage,
		FilePaths: []string{filePath},
	}, CompleteTaskReply{}
	call("Coordinator.CompleteTask", &args, &reply)
}

func WriteToOutFile(x int, buf []KeyValue) string {
	dir, _ := os.Getwd()
	filePath := dir + "/" + "mr-out-" + strconv.Itoa(x)
	//表示创建这个文件
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer file.Close()
	for _, kv := range buf {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	return filePath
}

func WriteToTempFile(x int, y int, buf []KeyValue) string {
	dir, _ := os.Getwd()
	filePath := dir + "/" + "mr-" + strconv.Itoa(x) + "-" + strconv.Itoa(y)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, kv := range buf {
		if err := enc.Encode(kv); err != nil {
			log.Fatal(kv, err.Error())
		}
	}
	return filePath
}

func ReadFromTempFile(files []string) []KeyValue {
	kvList := make([]KeyValue, 0)
	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err.Error())
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
	}
	return kvList

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
