package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*
go build -buildmode=plugin ../mrapps/wc.go
go run mrmaster.go pg-*.txt
*/

// Master structure
type Master struct {
	// number of jobs left
	mapsLeft, reducesLeft, nmap, nred int
	// 0 -> todo
	// 1 -> assigned
	// 2 -> partial
	// 3 -> complete
	// mapTaskCode corresponds numbering for map files
	mapTasks, reduceTasks, mapTaskCode map[string]int
	// mutexs
	mu sync.Mutex
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
// AssignJob gives requesting worker some job
// Assigns Map tasks, then reduce tasks
//
func (m *Master) AssignJob(id *int, job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mapsLeft != 0 {
		return m.giveJob(job, 0, m.mapTasks, m.mapTaskCode)
	}
	return m.giveJob(job, 1, m.reduceTasks, m.mapTaskCode)
}

func (m *Master) giveJob(job *Job, jobType int, taskMap, mapCode map[string]int) error {
	job.JobType = jobType
	job.Nmap = m.nmap
	job.Nred = m.nred
	for fname, status := range taskMap {
		if status == 0 {
			job.Fname = fname
			job.ID = m.mapTaskCode[fname]
			taskMap[fname] = 1
			go checkAlive(fname, taskMap)
			return nil
		}
	}
	return errors.New("No Unassigned MapReduce Tasks")
}

func checkAlive(fname string, taskMap map[string]int) {
	time.Sleep(10 * time.Second)
	fmt.Println("Checking status of ", fname)
	switch taskMap[fname] {
	case 1:
		fmt.Println("MR task failed with ", fname, " resetting to 0")
		taskMap[fname] = 0
	case 2:
		time.Sleep(3 * time.Second)
		if taskMap[fname] == 2 {
			fmt.Println("Write task failed with ", fname, " resetting to 0")
			taskMap[fname] = 0
		}
	}
	// if taskMap[fname] != 3 {
	// 	taskMap[fname] = 0
	// 	fmt.Println("Something wrong with ", fname)
	// }
}

// FinishedMap sets status of file to done mapping
func (m *Master) FinishedMap(mess *Message, _ *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mapTasks[mess.Fname] = 3
	fmt.Println("MAP Master setting ", mess.Fname, " as complete by ", mess.MyID)
	m.mapsLeft--
	// fmt.Println("Maps Left: ", m.mapsLeft)
	return nil
}

// FinishedReduce sets status of file to done mapping
func (m *Master) FinishedReduce(mess *Message, _ *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reduceTasks[mess.Fname] = 3
	fmt.Println("RED Master setting ", mess.Fname, " as complete by ", mess.MyID)
	m.reducesLeft--
	// fmt.Println("Maps Left: ", m.mapsLeft)
	return nil
}

// MapPartial sets status of file to partially done
func (m *Master) MapPartial(mess *Message, _ *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.mapTasks[mess.Fname] {
	case 1:
		// fmt.Println("Allowing ", mess.Fname)
		m.mapTasks[mess.Fname] = 2 // setting to partial stage
		// fmt.Println("\tMap ", mess.Fname, " partial by ", mess.MyID)
		return nil
	case 2:
		fmt.Println("Waiting ", mess.Fname)
		time.Sleep(2 * time.Second)
		if m.mapTasks[mess.Fname] == 2 {
			fmt.Println("Too long, rewriting ", mess.Fname)
			mess.Code = m.mapTaskCode[mess.Fname]
			return nil
		}
	case 3:
		fmt.Println("Too Slow ", mess.Fname)
		return errors.New("Already written")
	}

	// m.mapTasks[mess.Fname] = 2
	// fmt.Println("Master setting ", mess.Fname, " as complete")
	// m.mapsLeft--
	return nil
}

// ReducePartial sets status of file to partially done
func (m *Master) ReducePartial(mess *Message, _ *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.reduceTasks[mess.Fname] {
	case 1:
		// fmt.Println("Allowing ", mess.Fname)
		m.reduceTasks[mess.Fname] = 2 // setting to partial stage
		// fmt.Println("\tRed ", mess.Fname, " partial by ", mess.MyID)
		return nil
	case 2:
		fmt.Println("Waiting ", mess.Fname)
		time.Sleep(2 * time.Second)
		if m.reduceTasks[mess.Fname] == 2 {
			fmt.Println("Too long, rewriting ", mess.Fname)
			mess.Code = m.mapTaskCode[mess.Fname]
			return nil
		}
	case 3:
		fmt.Println("Too Slow ", mess.Fname)
		return errors.New("Already written")
	}

	// m.reduceTasks[mess.Fname] = 2
	// fmt.Println("RED Master setting ", mess.Fname, " as complete")
	// m.reducesLeft--
	return nil
}

//
// Done called from main/mrmaster.go periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.mapsLeft == 0 && m.reducesLeft == 0
}

//
// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
// called only once
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.mapsLeft = len(files)
	m.nmap = len(files)
	m.reducesLeft = nReduce
	m.nred = nReduce

	m.mapTasks = make(map[string]int)
	m.mapTaskCode = make(map[string]int)
	for i, fname := range files {
		m.mapTasks[fname] = 0
		m.mapTaskCode[fname] = i
	}

	m.reduceTasks = make(map[string]int)
	for i := 0; i < m.reducesLeft; i++ {
		fname := fmt.Sprintf("%v", i)
		m.reduceTasks[fname] = 0
	}
	// fmt.Println(m)
	m.server()
	return &m
}
