package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
type ByKey []KeyValue

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//atomic rename filename
func getmap_name(map_task_num int, index int) string {
	filename := fmt.Sprint("mr-", map_task_num, "-", index)
	return filename
}
func filename_map(filename string, task int, index int) {
	finalname := getmap_name(task, index)
	os.Rename(filename, finalname)
}
func filename_reduce(filename string, reduce_tasknum int) {
	finalname := fmt.Sprint("mr-out-", reduce_tasknum)
	os.Rename(filename, finalname)
}

//
// main/mrworker.go calls this function.
//
func perform_map(filename string, task_num int, nreduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("open file error:", err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("read error:", err)
	}
	file.Close()
	kva := mapf(filename, string(content))
	temper_files := []*os.File{}
	temper_filenames := []string{}
	encoders := []*json.Encoder{}
	for m := 0; m < nreduce; m++ {
		temp_file, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatal("ioutil error:", err)
		}
		temper_files = append(temper_files, temp_file)
		temp_filename := temp_file.Name()
		temper_filenames = append(temper_filenames, temp_filename)
		enc := json.NewEncoder(temp_file)
		encoders = append(encoders, enc)
	}
	for k, kv := range kva {
		index := ihash(kva[k].Key) % nreduce
		enc := encoders[index]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Encode error:", err)
		}
	}
	for _, f := range temper_files {
		f.Close()
	}
	for m := 0; m < nreduce; m++ {
		filename_map(temper_filenames[m], task_num, m)
	}

}
func perform_reduce(Nmap_tasknum int, Retask_num int, reducef func(string, []string) string) {
	kva := ByKey{}
	files := []string{}
	for i := 0; i < Nmap_tasknum; i++ {
		file := getmap_name(i, Retask_num)
		files = append(files, file)
	}
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			log.Fatal("open file error:", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	output_file, _ := ioutil.TempFile("", "")
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(output_file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	filename_reduce(output_file.Name(), Retask_num)
	//output_file.Close()

}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		call("Master.Get_task_handler", &args, &reply)
		task := reply.Tasktype
		nreduce := reply.NReduce_num
		task_num := reply.Tasknum
		Nmap_num := reply.NMap_num
		switch task {
		case Map:
			perform_map(reply.Filename, task_num, nreduce, mapf)
		case Reduce:
			perform_reduce(Nmap_num, task_num, reducef)
		case Done:
			os.Exit(0)
		default:
			log.Fatal("Unknow Type:Get_task error")
		}
		args_f := Finish_Args{}
		reply_f := Finish_Reply{}
		args_f.Tasknum = task_num
		args_f.Tasktype = reply.Tasktype
		call("Master.Finish_Task", &args_f, &reply_f)
	}
	//intermediate = append(intermediate, kva...)
	// uncomment to send the Example RPC to the master.
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//---------------------------------------------------------
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}
//----------------------------------------------------------
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
