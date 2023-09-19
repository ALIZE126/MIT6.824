package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
type Tasktype int

const (
	Map    = 1
	Reduce = 2
	Done   = 3
)

type WorkerArgs struct {
	//Worker_index int
}
type WorkerReply struct {
	Filename     string
	NReduce_num  int
	NMap_num     int
	Tasknum      int
	Reduce_index int
	Tasktype     Tasktype
}

type Finish_Args struct {
	Filename string
	Tasktype Tasktype
	Tasknum  int
}
type Finish_Reply struct {
}

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
