package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"


type Master struct {
	// Your definitions here.
	inputFiles []string
	interFiles []string
	outFiles []string
	nReduce int
	inputFileIndex int
    numMapsDone int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) TaskRequest(args *TaskArgs, reply *TaskReply) error {
    if m.numMapsDone < len(m.inputFiles){
        reply.IsMap = true
        reply.InputFile = m.inputFiles[m.inputFileIndex]
        reply.InputUID = m.inputFileIndex + 1
        m.inputFileIndex++
    } else {
        reply.IsMap = false
        reply.InputFile = m.interFiles[m.inputFileIndex]
    }
    if m.inputFileIndex == len(m.interFiles) {
        m.inputFileIndex = 0
    }
	return nil
}

func (m *Master) MapTaskDone(args *MapDoneArgs, reply *MapDoneReply) error {
    m.interFiles = append(m.interFiles, args.InterFile)
    m.numMapsDone = m.numMapsDone + 1
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false // Move to false!

    ret = m.numMapsDone == len(m.inputFiles) && m.inputFileIndex == len(m.interFiles)
	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.inputFiles = files
	m.nReduce = nReduce
	m.inputFileIndex = 0
	fmt.Printf("Files: %v, nReduce: %d\n", files, nReduce)
	m.server()
	return &m
}
