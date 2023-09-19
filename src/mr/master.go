package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mu               sync.Mutex
	Map_Files        []string
	Map_num          int
	Map_task         []bool
	Reduce_task      []bool
	Reduce_num       int
	cond             *sync.Cond
	MapTime_issue    []time.Time
	ReduceTime_issue []time.Time
	isdone           bool
	//Worker_state map[int]
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}
func (m *Master) Get_task_handler(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.NReduce_num = m.Reduce_num
	reply.NMap_num = m.Map_num
	for {
		mapdone := true
		for tasknum, done := range m.Map_task {
			if !done {
				if m.MapTime_issue[tasknum].IsZero() || time.Since(m.MapTime_issue[tasknum]).Seconds() > 10 {
					reply.Filename = m.Map_Files[tasknum]
					reply.Tasktype = Map
					reply.Tasknum = tasknum
					m.MapTime_issue[tasknum] = time.Now()
					return nil
				} else {
					mapdone = false
				}
			}
		}
		if !mapdone { // all map tasks have been sent to worker but have been finished  //must to wait
			m.cond.Wait()
		} else {
			break
		}
	}
	//Reduce_task
	for {
		reducedone := true
		for Retask_num, done := range m.Reduce_task {
			if !done {
				if m.ReduceTime_issue[Retask_num].IsZero() || time.Since(m.ReduceTime_issue[Retask_num]).Seconds() > 10 {
					reply.Tasknum = Retask_num
					reply.Tasktype = Reduce
					m.ReduceTime_issue[Retask_num] = time.Now()
					return nil
				} else {
					reducedone = false
				}
			}
		}
		if !reducedone {
			m.cond.Wait()
		} else {
			break
		}
	}
	reply.Tasktype = Done
	m.isdone = true
	return nil
}
func (m *Master) Finish_Task(args *Finish_Args, reply *Finish_Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.Tasktype {
	case Map:
		m.Map_task[args.Tasknum] = true
	case Reduce:
		m.Reduce_task[args.Tasknum] = true
	}
	m.cond.Broadcast()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isdone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.cond = sync.NewCond(&m.mu)
	m.Map_task = make([]bool, len(files))
	m.Reduce_task = make([]bool, nReduce)
	m.Map_Files = files
	m.Map_num = len(files)
	m.Reduce_num = nReduce
	m.MapTime_issue = make([]time.Time, len(files))
	m.ReduceTime_issue = make([]time.Time, nReduce)
	m.isdone = false
	m.server()
	go func() {
		for {
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	return &m
}
