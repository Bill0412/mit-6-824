package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"


type Coordinator struct {
	// Your definitions here.
	nReduce int
	files []string
	filePtr int
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

// DispatchTaskHandler
//
// the handler gets the worker request and handles it by sending back the task to be performed
//

func (c *Coordinator) DispatchTask(args *DispatchArgs, reply *DispatchReply) error {
	if c.filePtr >= len(c.files) {
		reply.filename = ""
		reply.success = false
	} else {
		reply.filename = c.files[c.filePtr]
		reply.success = true
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
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
	for i := 0; i != c.nReduce; i++ {
		file := "mr-out-" + strconv.Itoa(i)
		if _, err := os.Stat(file); err != nil {
			ret = false // if task i is not done
			break
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.files = files
	c.filePtr = 0

	c.server()
	return &c
}
