package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

	// Your worker implementation here.

	fmt.Println("Worker started")
	isDone := false
	var kva []KeyValue

	for isDone != true {

		// 1. notice coordinator and receive the file name
		// CallExample()
		filePathList := CallWorkerActive()
		if len(filePathList) == 0 {
			fmt.Println("receive zero file for map, finish mapping")
			break;
		}
		// 2. do map process
		

		for _, filePath := range filePathList {
			
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open %v", filePath)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", file)
			}
			file.Close()
			temp := mapf(filePath, string(content))
			kva = append(kva, temp...)
			fmt.Println("finish task from file " + filePath)
		}

		// 3. report done
		isDone = CallFinishMap(filePathList, kva)
		
	} 

	fmt.Printf("finish all map task")




	fmt.Println("Worker finished")

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallWorkerActive() []string {
	args := WorkerActiveArgs{}
	reply := WorkerActiveReply{}

	call("Coordinator.ActiveWorker", &args, &reply)

	return reply.FilePathList
}

func CallFinishMap(filePathList []string, kva []KeyValue) bool {
	args := FinishMapArgs{}
	reply := FinishMapReply{}
	args.FilePathList = append(args.FilePathList, filePathList...)
	args.kva = append(args.kva, kva...)

	call("Coordinator.FinishMap", &args, &reply)

	return reply.IsDone
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
// usually returns true.
// returns false if something goes wrong.
//
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
