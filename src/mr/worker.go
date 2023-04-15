package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.直接将mrsequential.go的排序方法复制过来即可
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//使用轮询机制向master请求或报告
	for {
		reply := acquireTask()
		switch reply.Status {
		case DoMap:
			intermediatefiles, ok := doMapTask(mapf, reply)
			if ok {
				//fmt.Printf("MapTask-%v succeed\n", reply.MapInfo.Id)
				//通知master，map任务完成
				args := &RPCargs{
					Status: DoMap,
					Files:  intermediatefiles,
					Id:     "DoMap" + strconv.Itoa(reply.MapInfo.Id),
				}
				call("Master.ReportTask", args, reply)
			} else {
				fmt.Printf("MapTask-%v failed\n", reply.MapInfo.Id)
			}
		case DoReduce:
			finaltempfile, ok := doReduceTask(reducef, reply)
			if ok {
				//fmt.Printf("ReduceTask-%v succeed\n", reply.ReduceInfo.Id)
				//通知master，reduce任务完成
				args := &RPCargs{
					Status: DoReduce,
					Files:  []string{finaltempfile, strconv.Itoa(reply.ReduceInfo.Id)},
					Id:     "DoReduce" + strconv.Itoa(reply.ReduceInfo.Id),
				}
				call("Master.ReportTask", args, reply)
			} else {
				fmt.Printf("ReduceTask-%v failed\n", reply.ReduceInfo.Id)
			}
		case TaskWait:
			time.Sleep(time.Millisecond)
		case AllDone:
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func acquireTask() *RPCreply {
	args := RPCargs{}
	reply := RPCreply{}

	err := call("Master.RequestTask", &args, &reply)
	if !err {
		return nil
	}
	return &reply
}

//做map任务，返回一组中间文件，以及处理结果状态
func doMapTask(mapf func(string, string) []KeyValue, mtask *RPCreply) ([]string, bool) {
	filename := mtask.MapInfo.File
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("open %v failed!!!", filename)
		log.Fatalf("open %v failed!", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("read %v failed!", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	return saveToDisk(kva, mtask.MapInfo.Id, mtask.NReduce)
}

//将中间键值对写入中间文件，通过hash写入对应的reduce列中
func saveToDisk(kva []KeyValue, mtaskid int, nReduce int) ([]string, bool) {
	//每个reduce任务存储多个键值对
	rtable := make([][]KeyValue, nReduce)
	for _, pair := range kva {
		id := ihash(pair.Key) % nReduce
		rtable[id] = append(rtable[id], pair)
	}

	//创建中间文件列表
	intermediatefiles := make([]string, 0)

	for idx, kvrow := range rtable {
		//为map任务创建NRdeuce个中间文件
		intermediatefile := "mr-" + strconv.Itoa(mtaskid) + "-" + strconv.Itoa(idx)
		//创建此文件
		file, _ := os.OpenFile(intermediatefile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)

		enc := json.NewEncoder(file)
		err := enc.Encode(kvrow)
		if err != nil {
			fmt.Printf("write to %v failed!\n", intermediatefile)
			return intermediatefiles, false
		}
		file.Close()
		intermediatefiles = append(intermediatefiles, intermediatefile)

	}
	return intermediatefiles, true
}

func doReduceTask(reducef func(string, []string) string, rtask *RPCreply) (string, bool) {
	filenames := rtask.ReduceInfo.Files

	kva := []KeyValue{}

	//依此读入reduce任务对应的每个中间文件，获得所有的kv对
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("open %v failed\n", filename)
			log.Fatalf("open %v failed!", filename)
		}

		dec := json.NewDecoder(file)

		for {
			var kv []KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv...)
		}

		//删除中间文件
		/*if file != nil {
			os.Remove(file.Name())
		}*/
	}

	//排序，按照key从小到大
	sort.Sort(ByKey(kva))

	values := make([]string, 0)

	//oname := "mr-out-" + strconv.Itoa(rtask.ReduceInfo.Id)
	ofile, err := ioutil.TempFile("./", "temp-reduce-"+strconv.Itoa(rtask.ReduceInfo.Id))
	if err != nil {
		fmt.Printf("temp file creation failed\n")
	}

	//根据key的不同，需要分组执行reducf函数
	for i, kv := range kva {
		values = append(values, kv.Value)
		//下一个与当前key相同，则还不是同组的最后一个
		if i+1 < len(kva) && kva[i].Key == kva[i+1].Key {
			continue
		}

		res := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, res)

		//当前key对应的values已经执行完毕，继续收集下一个key的values
		values = values[0:0]
	}

	//os.Rename(ofile.Name(), oname)

	for _, filename := range filenames {
		//删除中间文件
		file, _ := os.Open(filename)
		if file != nil {
			os.Remove(file.Name())
		}
	}

	return ofile.Name(), true
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
