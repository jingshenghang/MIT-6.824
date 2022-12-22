package mr

import (
	"crypto/rand"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func GetRandomString2(n int) string {
	randBytes := make([]byte, n/2)
	rand.Read(randBytes)
	return fmt.Sprintf("%x", randBytes)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	fmt.Println("Worker started")
	isMapDone := false
	var kva []KeyValue
	// var reduceResult []KeyValue

	for isMapDone != true {

		kva = make([]KeyValue, 0)
		// 1. notice coordinator and receive the file name
		// CallExample()
		filePathList := CallWorkerActive()
		fmt.Printf("receive %d mapping file\n", len(filePathList))
		if len(filePathList) == 0 {
			
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
		}

		// 3. report done
		isMapDone = CallFinishMap(filePathList, kva)
		
	} 

	fmt.Println("finish all map task")
	

	// 4. start reduce task
	fmt.Println("start reduce task")
	

	// 5. type Sort
	// sort.Sort(ByKey(kva))

	// 6. reduce

	isReduceDone := false

	for !isReduceDone {
		reply := CallStartReduce()
		if reply.IsDone {
			isReduceDone = true;
			break;
		} 

		// oname := "mr-out-0"
		// ofile, _ := os.Create(oname)

		fmt.Printf("start reduce task %d with %d kva\n", reply.ReduceId, len(reply.Kva))

		oname := "reduce-result-" + GetRandomString2(5)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(reply.Kva) {
			j := i + 1
			for j < len(reply.Kva) && reply.Kva[j].Key == reply.Kva[i].Key {
				j++
			}
			if (ihash(reply.Kva[i].Key) % reply.NReduce) == reply.ReduceId {
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, reply.Kva[k].Value)
				}
				output := reducef(reply.Kva[i].Key, values)
			
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", reply.Kva[i].Key, output)
			} 
	
			i = j
		}

		ofile.Close()
		fmt.Printf("finish reduce task %d\n", reply.ReduceId)

		CallFinishReduce(oname, reply.ReduceId)

	}



	
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
	args.Kva = append(args.Kva, kva...)

	call("Coordinator.FinishMap", &args, &reply)

	return reply.IsDone
}	

func CallStartReduce() StartReduceReply {
	args := StartReduceArgs{}
	reply := StartReduceReply{}

	call("Coordinator.StartReduce", &args, &reply)

	return reply
}

func CallFinishReduce(oname string, reduceId int)  {
	args := FinishReduceArgs{}
	args.OutputFileName = oname
	args.ReduceId = reduceId
	reply := FinishReduceReply{}
	call("Coordinator.FinishReduce", &args, &reply)
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
