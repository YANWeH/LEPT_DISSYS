package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	DoMap int = iota
	DoReduce
	TaskWait
	AllDone
)

type MapTask struct {
	Id   int    //当前map任务id
	File string //一个map任务执行一个文件
}

type ReduceTask struct {
	Id    int      //当前reduce任务id
	Files []string //一个reduce任务执行多个文件
}

type Master struct {
	// Your definitions here.
	NReduce int //reduce任务量
	NMap    int //map任务量
	Status  int //当前master的状态 0-DoMap 1-DoReduce 2-TaskWait 3-AllDone

	QueMap    chan MapTask    //存储map任务的队列，可保证线程安全
	QueReduce chan ReduceTask //存储reduce任务的队列，可保证线程安全

	IntermediateFiles [][]string //每个map任务完成后，worker传递给master NReduce个中间文件
	TempResultFiles   []string   //每个reduce任务完成后，worker传递给master一个结果文件

	mutex sync.Mutex //防止race condition

	workermap map[string]workerState //记录每个worker状态
}

//实现crash recovery
//记录worker工作状态以及工作时间
//var workerId = 0

type workerState struct {
	Status     int
	StartTime  time.Time
	MapInfo    MapTask
	ReduceInfo ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

//worker向master请求一个任务，master根据状态回应请求
func (m *Master) RequestTask(args *RPCargs, reply *RPCreply) error {
	m.mutex.Lock() //master根据自身状态回应worker，加锁，防止访问时master状态被其他worker改变
	defer m.mutex.Unlock()

	reply.NReduce = m.NReduce

	switch m.Status {
	case DoMap:
		//查看map队列中是否还有数据，如果没有，则说明map任务分配完了，但还没收到worker的结果报告
		if len(m.QueMap) == 0 {
			m.Status = TaskWait
			reply.Status = TaskWait
		} else {
			//分配map任务，从map队列中取出一个给worker
			reply.MapInfo = <-m.QueMap
			reply.Status = DoMap

			//记录这个任务的开始状态

			//fmt.Printf("当前时间：%v\n", time.Now())

			stateworker := workerState{
				StartTime: time.Now(),
				Status:    DoMap,
				MapInfo:   reply.MapInfo,
			}
			mapidx := "DoMap" + strconv.Itoa(reply.MapInfo.Id)
			m.workermap[mapidx] = stateworker
			//workerId++
		}
	case DoReduce:
		//查看reduce队列中是否还有数据，如果没有，则说明reduce任务分配完了，但还没收到worker的结果报告
		if len(m.QueReduce) == 0 {
			m.Status = TaskWait
			reply.Status = TaskWait
		} else {
			//分配reduce任务，从reduce队列中取出一个个worker
			reply.ReduceInfo = <-m.QueReduce
			reply.Status = DoReduce

			//记录这个任务的开始状态
			stateworker := workerState{
				StartTime:  time.Now(),
				Status:     DoReduce,
				ReduceInfo: reply.ReduceInfo,
			}
			mapidx := "DoReduce" + strconv.Itoa(reply.ReduceInfo.Id)
			m.workermap[mapidx] = stateworker
			//workerId++
		}
	case TaskWait:
		//如果master处于TaskWait状态，任务可能crash或者还在规定时间内但未完成
		//遍历workermap，查看目前还在进行中的worker状态是否超过规定时间
		for id, ws := range m.workermap {
			currentTime := time.Now()
			//fmt.Printf("当前时间：%v", currentTime)

			//timepassed := currentTime.Second() - ws.StartTime.Second()

			//timepassed := currentTime.Unix() - ws.StartTime.Unix()

			timepassed := currentTime.Sub(ws.StartTime)
			seconds := int(timepassed.Seconds())
			//fmt.Printf("时间差：%v\n", seconds)

			//超过10秒未报告结果，则master认为此worker crashed
			if seconds > 10 {
				m.Status = ws.Status
				switch ws.Status {
				case DoMap:
					m.QueMap <- ws.MapInfo
				case DoReduce:
					m.QueReduce <- ws.ReduceInfo
				}
				delete(m.workermap, id)
			}
		}

		reply.Status = TaskWait
	default:
		reply.Status = AllDone
	}
	return nil
}

//worker向master报告任务结果,要么是map任务完成，要么是reduce任务完成
func (m *Master) ReportTask(args *RPCargs, reply *RPCreply) error {
	//在报告的过程中，可能修改master的状态，因此需要锁防止并发访问
	m.mutex.Lock()
	defer m.mutex.Unlock()

	//此任务已完成，删除此任务的状态信息
	if _, ok := m.workermap[args.Id]; !ok {
		return nil
	}

	delete(m.workermap, args.Id)

	//根据worker做的任务状态，将任务结果放入对应结果集中
	switch args.Status {
	case DoMap:
		m.IntermediateFiles = append(m.IntermediateFiles, args.Files)

		//完成了所有map任务，master该进入下一个阶段reduce
		if len(m.IntermediateFiles) == m.NMap {
			m.MakeReduceTask()
			m.Status = DoReduce
			//fmt.Printf("start ReduceTask!\n")
		}
	case DoReduce:
		m.TempResultFiles = append(m.TempResultFiles, args.Files[0])
		//修改临时文件为最终结果文件
		finalname := "mr-out-" + args.Files[1]
		os.Rename(args.Files[0], finalname)
		//完成了所有reduce任务，master进入下一个阶段allDone
		if len(m.TempResultFiles) == m.NReduce {
			m.Status = AllDone
		}
	}
	return nil
}

//将map生成的M×R个中间文件按照对应的reduce编号将map文件表格的每一列放入reduce任务中
func (m *Master) MakeReduceTask() {
	for i := 0; i < m.NReduce; i++ {
		//创建一个reduce任务
		rtask := ReduceTask{
			Id: i,
		}
		for j := 0; j < m.NMap; j++ {
			rtask.Files = append(rtask.Files, m.IntermediateFiles[j][i])
		}
		m.QueReduce <- rtask
	}
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
	ret := false

	// Your code here.
	ret = m.Status == AllDone
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	//初始化master结构体
	m.NReduce = nReduce
	m.NMap = len(files)

	m.QueMap = make(chan MapTask, m.NMap)
	m.QueReduce = make(chan ReduceTask, m.NReduce)

	for i, f := range files {
		mtask := MapTask{
			Id:   i,
			File: f,
		}
		m.QueMap <- mtask
	}

	m.Status = DoMap

	m.workermap = make(map[string]workerState)

	m.server()
	return &m
}
