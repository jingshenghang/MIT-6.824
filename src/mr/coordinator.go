package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

var filePathList []string
var index int
var IntermediateMap []KeyValue
var flag bool = false

func DeleteSlice(a []string, elem string) []string {
	j := 0
	for _, v := range a {
		if v != elem {
			a[j] = v
			j++
		}
	}
	return a[:j]
}

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ActiveWorker(args *WorkerActiveArgs, reply *WorkerActiveReply) error {

	if len(filePathList) > 0 {
		index = (index + 1) % len(filePathList)
		reply.FilePathList = append(reply.FilePathList, filePathList[index])
	} 
	
	return nil
}


// 传入完成的文件id，kva结果
func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {

	for _, file := range args.FilePathList {
			filePathList = DeleteSlice(filePathList, file)
	}
	IntermediateMap = append(IntermediateMap, args.Kva...)

	// fmt.Println("Remining " + strconv.Itoa(len(filePathList)) + " files")
	
	if len(filePathList) == 0 {
		reply.IsDone = true
	} else {
		reply.IsDone = false
	}

	return nil
}

func (c *Coordinator) StartReduce(args *StartReduceArgs, reply *StartReduceReply) error {

	reply.Kva = IntermediateMap
	return nil
}

func (c *Coordinator) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {

	flag = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	fmt.Println("Server listening...")
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
	if len(filePathList) == -1 { // 需不需要加锁？ // 需要修改
		ret = true
	}

	// for test
	if flag == true {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
		// Your code here.


	fmt.Println("Starting coordinator")
	// initialization

	c := Coordinator{}
	filePathList = files
	index = 0

	c.server()

	fmt.Println("Coordinator started")
	return &c
}
