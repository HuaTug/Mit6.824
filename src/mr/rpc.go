package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 定义任务
type Task struct {
	TaskStage     int
	TaskId        int
	ReduceNum     int
	Intermediates []string
	FileName      string
	StartTime     time.Time
}

const (
	Map            = 0
	MapComplete    = 1
	Reduce         = 2
	ReduceComplete = 3
	Wait           = 4
	Exit           = 5
)

// 获取事务
//进行RPC通信时 客户端向服务端发送的请求 与 服务端向客户端回应的处理
type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task Task
}

// 通知完成事务
type CompleteTaskArgs struct {
	TaskId    int
	Stage     int
	FilePaths []string
}

type CompleteTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
