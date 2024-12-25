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
	interFileIndex int
    numMapsDone int
    numReduceDone int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) TaskRequest(args *TaskArgs, reply *TaskReply) error {
	reply.IsDone = false
	reply.NoReq = false
	fmt.Printf("TaskRequested: numMapsDone = %d, numInputFiles = %d\n", m.numMapsDone, len(m.inputFiles))
	if m.Done() {
		reply.NoReq = true
	}else if m.numMapsDone < len(m.inputFiles) {
        reply.IsMap = true
		fmt.Printf("Sending file #%d of %d files.\n", m.inputFileIndex, len(m.inputFiles));
        reply.InputFiles = []string{m.inputFiles[m.inputFileIndex]}
        reply.InputUID = m.inputFileIndex + 1
        m.inputFileIndex = reply.InputUID
		fmt.Printf("Input File Index: %d\n", m.inputFileIndex);
    } else if reply.InputUID < len(m.interFiles) && len(m.inputFiles) == len(m.interFiles){
        reply.IsMap = false
        reply.InputFiles = m.interFiles
        reply.InputUID = m.interFileIndex + 1
        m.interFileIndex = reply.InputUID
    } else {
		reply.NoReq = true
	}
	return nil
}

func (m *Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	fmt.Printf("TaskDone reported by map ? %b\n", args.IsMap)
	if(args.IsMap) {
		m.interFiles = append(m.interFiles, args.OutputFile)
		m.numMapsDone = m.numMapsDone + 1
		fmt.Printf("Finished map job, interfiles: %v\n Num done %d\n", m.interFiles, m.numMapsDone)
	} else {
		m.outFiles = append(m.outFiles, args.OutputFile)
		m.numReduceDone = m.numReduceDone + 1
		fmt.Printf("Finished reduce job, outFiles: %v\n Num done %d\n", m.outFiles, m.numReduceDone)
	}
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

    ret = m.numMapsDone >= len(m.inputFiles) && m.numReduceDone >= len(m.interFiles)

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
