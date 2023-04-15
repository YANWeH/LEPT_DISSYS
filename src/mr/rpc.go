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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RPCargs struct {
	Status int      //worker传递给master的状态只能是DoMap和DoReduce两个状态其中之一
	Files  []string //map任务完成会传递NReduce个中间文件，reduce任务完成会传递一个临时结果文件即Files[0]
	Id     string
}

type RPCreply struct {
	Status int

	MapInfo    MapTask    //master给worker分配一个map任务
	ReduceInfo ReduceTask //master给worker分配一个reduce任务

	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
