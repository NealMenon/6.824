package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// ByKey used for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var myID int = rand.New(rand.NewSource(time.Now().UnixNano())).Intn(200)

//
// Worker begins distributed execution
// main/mrworker.go calls this function.
// go run mrworker.go wc.so
// go run mrworker.go indexer.so
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Println("\n\nNEW WORKER -> ", myID)
	const maxConcurrentOps = 1

	wait := false
	for {
		var wg sync.WaitGroup
		for i := 0; i < maxConcurrentOps; i++ {
			wg.Add(1)
			go func(id int) {
				task := Job{}
				// send the RPC request, wait for the reply.
				err := call("Master.AssignJob", id, &task)
				if err != nil {
					wait = true
				} else {
					fmt.Printf("%v received job from Master -> Name: %v Function: %v \n", myID, task.Fname, task.JobType)
					time.Sleep(time.Second)
					if task.JobType == 0 { // MAP
						doMapTask(mapf, task.Fname, task.ID, task.Nred)
					} else { // REDUCE
						doReduceTask(reducef, task.Fname, task.Nmap, task.Nred)
					}
				}
				wg.Done()
			}(i)
			if wait {
				// fmt.Println("Trying again in 2 seconds")
				time.Sleep(2 * time.Second)
				wait = false
			}
		}
		time.Sleep(200 * time.Millisecond)
		wg.Wait()
	}
}

func doReduceTask(reducef func(string, []string) string, filename string, nmap, nred int) {
	fmt.Printf("Reduce name: %v\n", filename)

	reduceData := make([]KeyValue, 0)
	for i := 0; i < nmap; i++ {
		intfn := fmt.Sprintf("mr-tmp/mr-int-%v-%v", i, filename)
		kvs, _ := scanLines(intfn)
		reduceData = append(reduceData, kvs...)
	}
	// fmt.Printf("Reduce %v has %v lines\n", filename, len(reduceData))

	sort.Sort(ByKey(reduceData))

	outdata := make([][]string, 0)
	i := 0
	for i < len(reduceData) {
		j := i + 1
		for j < len(reduceData) && reduceData[j].Key == reduceData[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, reduceData[k].Value)
		}
		output := reducef(reduceData[i].Key, values)
		outdata = append(outdata, []string{reduceData[i].Key, output})
		// this is the correct format for each line of Reduce output.
		// fmt.Fprintf(ofile, "%v %v\n", reduceData[i].Key, output)
		i = j
	}
	if !notifyReducePartial(filename) {
		fmt.Println("Someone else has already written to memory -> ", filename)
	} else {
		// fmt.Println("Fresh ", filename)
		oname := fmt.Sprintf("mr-out-%v", filename)
		ofile, _ := os.Create(oname)
		for _, row := range outdata {
			fmt.Fprintf(ofile, "%v %v\n", row[0], row[1])
		}
		ofile.Close()
	}

	completedReduce(filename)
}
func notifyReducePartial(fname string) bool {
	mess := Message{}
	mess.Code = 1
	mess.Fname = fname
	mess.MyID = myID
	err := call("Master.ReducePartial", &mess, &mess)
	if err != nil {
		return false
	}
	return true
}

func doMapTask(mapf func(string, string) []KeyValue, filename string, jobID, nred int) {
	fmt.Printf("Map name: %v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	intfname := fmt.Sprintf("mr-int-%v", jobID)
	inter := make(map[int][]KeyValue)
	// put each entry in appropriate bucket
	for _, kv := range kva {
		dest := ihash(kv.Key) % nred
		inter[dest] = append(inter[dest], kv)
	}
	fmt.Println("Finished splitting ", filename)
	// time.Sleep(10 * time.Second)

	if !notifyMapPartial(filename) {
		fmt.Println("Someone else has already written to memory -> ", filename)
	} else {
		// fmt.Println("Fresh ", filename)
		var wg sync.WaitGroup
		for k, v := range inter {
			// fmt.Printf("%v has %v\n", k, len(v))
			wg.Add(1)
			go func(k int, data []KeyValue) {
				printLines(fmt.Sprintf("mr-tmp/%v-%v", intfname, k), data)
				// fmt.Printf("Finished writing %v with lines %v \n", fmt.Sprintf("mr-tmp/%v-%v.txt", intfname, k), len(data))
				wg.Done()
			}(k, v)
		}
		wg.Wait()
		// fmt.Println("FINISH ->  ", filename)

		completedMap(filename)
		// time.Sleep(2 * time.Second)
	}

}

func notifyMapPartial(fname string) bool {
	mess := Message{}
	mess.Code = 1
	mess.MyID = myID
	mess.Fname = fname
	err := call("Master.MapPartial", &mess, &mess)
	if err != nil {
		// cleanUp(mess.Code)
		return false
	}
	// if mess.Code
	return true
}

func completedMap(fname string) {
	// fmt.Println("Worker Finished       ", fname)
	mess := Message{}
	mess.Code = 0
	mess.MyID = myID
	mess.Fname = fname
	call("Master.FinishedMap", &mess, &mess)
}

func completedReduce(fname string) {
	// fmt.Println("Worker Finished       ", fname)
	mess := Message{}
	mess.Code = 0
	mess.MyID = myID
	mess.Fname = fname
	call("Master.FinishedReduce", &mess, &mess)
}

func printLines(filePath string, values interface{}) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	rv := reflect.ValueOf(values)
	if rv.Kind() != reflect.Slice {
		return errors.New("Not a slice")
	}
	for i := 0; i < rv.Len(); i++ {
		fmt.Fprintln(f, rv.Index(i).Interface())
	}
	return nil
}

func scanLines(path string) ([]KeyValue, error) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error in reading: ", path)
		return nil, err
	}
	defer file.Close()

	var kvs []KeyValue
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		kv := KeyValue{}
		line := strings.Split(strings.Trim(scanner.Text(), "{}"), " ")
		kv.Key = line[0]
		kv.Value = line[1]
		kvs = append(kvs, kv)
	}
	return kvs, scanner.Err()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err, "\nMaster is no longer active")
		log.Fatal("Master is no longer active \tShutting Down\n\n")
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
