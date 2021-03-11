package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// KeyValue slice is returned on map calls
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// func (kv KeyValue) String() string {
// 	return fmt.Sprintf("%v %v", kv.Key, kv.Value)
// }

// Message passes key data between master and worker
type Message struct {
	Code  int // 0 for complete		1 for Partially done	2 for cleanup
	Fname string
	MyID  int
}

//
// Job is information shared from Master to Worker
//
type Job struct {
	ID      int // unique ID for Map tasks
	Fname   string
	JobType int // 0 for map, 1 for reduce
	Nmap    int // total number of map tasks
	Nred    int // total number of reduce tasks
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
