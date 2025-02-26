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

type WorkerActiveArgs struct {
	X int
}

type WorkerActiveReply struct {
	FilePathList []string
}

type FinishMapArgs struct {
	FilePathList []string
	Kva []KeyValue
}

type FinishMapReply struct {
	IsDone bool
}

type StartReduceArgs struct {
	
}

type StartReduceReply struct {
	// Kva []KeyValue
	NReduce int
	ReduceId int
	IsDone bool
	KvaPath string
}

type FinishReduceArgs struct {
	OutputFileName string
	ReduceId int
}

type FinishReduceReply struct {
	Kva []KeyValue
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

