package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "strings"
import "io/ioutil"
import "os"


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

func run_map(mapf func(string, string) []KeyValue, reducef func(string, []string) string, args *TaskArgs, reply *TaskReply) bool {


	parsedInput := strings.Replace(reply.InputFile, ".txt", "", 1)
	output := parsedInput + "_out_" + strconv.Itoa(reply.InputUID) + ".txt"
	content, err := ioutil.ReadFile(reply.InputFile)
	if err != nil {
		fmt.Printf("Failed to read file: %s\n", err)
		return false
	}
	result := mapf(reply.InputFile, string(content))
	fmt.Printf("Ran a mapf on %s, to %s\n", reply.InputFile, output)
	for _, kv := range result {
		fmt.Printf("%d -> Key: %s, Value: %s\n", reply.InputUID, kv.Key, kv.Value)
	}

	file, err := os.Create(output)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return false
	}
	defer file.Close() // Ensure the file is closed when the program exits

	// Write each KeyValue pair to the file
	for _, kv := range result {
		_, err := fmt.Fprintf(file, "%s %s\n", kv.Key, kv.Value)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return false
		}
	}

	fmt.Println("Data successfully written to", output)

	map_args := MapDoneArgs{}
	map_args.InterFile = output
	map_reply := MapDoneReply{}
	call("Master.MapTaskDone", &map_args, &map_reply)

	return true

}

func execute_task(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Master.TaskRequest", &args, &reply)

	// Your worker implementation here.
	if(reply.IsMap) {
		run_map(mapf, reducef, &args, &reply)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
