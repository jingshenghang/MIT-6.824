package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
)

var filePathList []string
var mapIndex int
var IntermediateMap []KeyValue // 这个不能调用mr包的吧？
var flag bool = false
var mux sync.Mutex
var isReduceFinish bool = false
var reduceList []bool
var reduceIndex int
var finishReduceCount int



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
	nReduce int
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

	mux.Lock()
	if len(filePathList) > 0 {
		mapIndex = (mapIndex + 1) % len(filePathList)
		reply.FilePathList = append(reply.FilePathList, filePathList[mapIndex])
	} 
	mux.Unlock()
	
	return nil
}


// 传入完成的文件id，kva结果
func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {

	mux.Lock()
	
	initLen := len(filePathList)
	for _, file := range args.FilePathList {
			filePathList = DeleteSlice(filePathList, file)
	}
	if len(filePathList) < initLen {
		IntermediateMap = append(IntermediateMap, args.Kva...)
	}

	// fmt.Println("Remining " + strconv.Itoa(len(filePathList)) + " files")
	
	if len(filePathList) == 0 {
		reply.IsDone = true
	} else {
		reply.IsDone = false
	}
	mux.Unlock()

	if reply.IsDone{
		sort.Sort(ByKey(IntermediateMap))
	}

	return nil
}

func (c *Coordinator) StartReduce(args *StartReduceArgs, reply *StartReduceReply) error {
	
	if !isReduceFinish{
		mux.Lock()

		tryTimes := 0

		for tryTimes < c.nReduce {
			fmt.Printf("Reduce tryTimes = %d\n", tryTimes)
			reduceIndex = (reduceIndex + 1) % c.nReduce
			if reduceList[reduceIndex] {
				tryTimes++
			} else {
				reply.Kva = IntermediateMap
				reply.NReduce = c.nReduce
				reply.ReduceId = reduceIndex
				break
			}
		}

		if(tryTimes == c.nReduce) {
			reply.IsDone = true
		}

		mux.Unlock()
 	} else {
		reply.IsDone = true
		isReduceFinish = true
		
	}

	return nil
}

func (c *Coordinator) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {

	mux.Lock()
	if !reduceList[args.ReduceId] {
		

		reduceList[args.ReduceId] = true
		finishReduceCount++

		// 重命名文件
		err1 := os.Rename(args.OutputFileName, "mr-out-" + strconv.Itoa(args.ReduceId))
		if err1 != nil {
			 panic(err1)
		} else {
			 fmt.Printf("save reduce task %d\n", args.ReduceId)
		}
	
		if finishReduceCount == c.nReduce {
			isReduceFinish = true
		}

	
	} else {
		os.Remove(args.OutputFileName)
	}
	mux.Unlock()

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
	if flag {
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
	mapIndex = 0
	reduceIndex = 0
	reduceList = make([]bool, nReduce)
	c.nReduce = nReduce
	finishReduceCount = 0

	c.server()

	fmt.Println("Coordinator started")
	return &c
}
