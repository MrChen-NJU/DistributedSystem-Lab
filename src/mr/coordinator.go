package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mutex sync.Mutex

type TaskOperate uint32

const (
	to_map TaskOperate = iota
	to_reduce
)

type TaskState uint32

// task状态
const (
	idle TaskState = iota
	mapping
	reducing
	done
)

type CoordinatorState uint32

// coordinator状态
const (
	MAP CoordinatorState = iota
	REDUCE
	EXIT
)

//
// 当前任务的元数据
//
type TaskMeta struct {
	task_id       int      // 任务id：0 ——> n-1
	task_file     []string // 该任务的原始文件名
	intermediates []string // map处理完成后的中间数据所在文件的文件名

	task_state        TaskState // 当前任务的状态：idle\mmap\reduce\done
	map_worker        int       // 执行map任务的worker
	map_start_time    time.Time
	reduce_worker     int // 执行reduce任务的worker
	reduce_start_time time.Time
}

//
// 自定义的Task
//
type Task struct {
	task_id       int      // 任务id：0 ——> n-1
	task_file     []string // 该任务的文件名
	intermediates []string // 该任务经过map处理后的中间文件的文件名

	task_operate TaskOperate // 当前任务要执行的操作：to_map\to_reduce
	task_worker  int         // 被指派当前任务的worker
	start_time   time.Time   // 记录任务开始的时间

	reduce_bucket int // 进行reduce时处理的桶号
	NReduce       int // reduce桶的个数
}

//
// 自定义的Coordinator
//
type Coordinator struct {
	// Your definitions here.
	NReduce int              // reduce桶的个数
	phase   CoordinatorState // coordinator当前所处的状态：map\reduce\exit

	task_metas         []TaskMeta  // 文件名与对应任务的映射
	task_queue         *list.List  // 任务队列
	map_task_worker    map[int]int // 执行的map任务号与worker id的映射
	reduce_task_worker map[int]int // 执行的reduce任务号与worker id的映射

	map_total    int    // 总的需要map处理的个数，等于待处理的文件的个数
	map_count    []bool // 记录已完成的map任务
	reduce_total int    // 总的需要reduce处理的个数，等于NReduce
	reduce_count []bool // 记录已完成的reduce任务
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

//
// 检查数组中是否全为true
//
func check_all_true(count []bool) bool {
	for _, ck := range count {
		if !ck {
			return false
		}
	}
	return true
}

//
// 被worker通过RPC调用来向coordinator请求任务
//
func (c *Coordinator) AssignTask(args *int, reply *Task) error {
	mutex.Lock()
	defer mutex.Unlock()
	worker_id := *args

	if c.task_queue.Len() > 0 {
		if c.phase == MAP && c.task_queue.Front().Value.(*Task).task_operate == to_map {
			// coordinator阶段为MAP且任务队列里的任务为map任务
			*reply = *(c.task_queue.Front().Value.(*Task)) // 初始化reply
			tid := (*reply).task_id
			c.task_queue.Remove(c.task_queue.Front())
			c.map_task_worker[tid] = worker_id // 建立任务id和worker id的映射

			c.task_metas[tid].task_state = mapping
			c.task_metas[tid].map_worker = worker_id
			c.task_metas[tid].map_start_time = time.Now()

			(*reply).task_worker = worker_id
			(*reply).start_time = c.task_metas[tid].map_start_time

		} else if c.phase == REDUCE && c.task_queue.Front().Value.(*Task).task_operate == to_reduce {
			// coordinator阶段为REDUCE且任务队列里的任务为reduce任务
			*reply = *(c.task_queue.Front().Value.(*Task)) // 初始化reply
			tid := (*reply).task_id
			c.task_queue.Remove(c.task_queue.Front())
			c.reduce_task_worker[tid] = worker_id // 建立任务id和worker id的映射

			c.task_metas[tid].task_state = reducing
			c.task_metas[tid].reduce_worker = worker_id
			c.task_metas[tid].reduce_start_time = time.Now()

			(*reply).task_worker = worker_id
			(*reply).start_time = c.task_metas[tid].reduce_start_time
		}
	}
	return nil
}

//
// 被worker通过RPC调用来通知coordinator任务完成
//
func (c *Coordinator) ComplateTask(args *ExampleArgs, result *Task) error {
	mutex.Lock()
	defer mutex.Unlock()
	task_id := result.task_id
	switch result.task_operate {
	case to_map:
		if !c.map_count[task_id] {
			copy(c.task_metas[task_id].intermediates, result.intermediates)
			delete(c.map_task_worker, task_id)
			c.map_count[task_id] = true
			if check_all_true(c.map_count) {
				// 所有map任务执行完成后，coordinator进入REDUCE阶段：准备nReduce个reduce任务
				c.phase = REDUCE
				for i := 0; i < c.NReduce; i++ {
					intermediate_file_names := make([]string, 0)
					for j := 0; j < c.map_total; j++ {
						intermediate_file_names = append(intermediate_file_names, fmt.Sprintf("mr-out-%d-%d.json", j, i))
					}
					c.task_queue.PushBack(Task{
						task_id:       i,
						task_file:     intermediate_file_names,
						task_operate:  to_reduce,
						task_worker:   -1,
						start_time:    time.Now(),
						reduce_bucket: i,
						NReduce:       c.NReduce,
					})
				}
			}
		}
	case to_reduce:
		bucket_id := result.reduce_bucket
		if !c.reduce_count[bucket_id] {
			delete(c.reduce_task_worker, task_id)
			c.reduce_count[task_id] = true
			if check_all_true(c.reduce_count) {
				// 所有reduce任务执行完成后，coordinator进入EXIT阶段
				c.phase = EXIT
				for i := range c.task_metas {
					c.task_metas[i].task_state = done
				}
			}
		}
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
	task_num := len(files)
	c.NReduce = nReduce
	c.phase = MAP
	c.map_count = make([]bool, task_num)
	c.map_total = task_num
	c.reduce_count = make([]bool, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduce_count[i] = false
	}
	c.reduce_total = nReduce
	for i := 0; i < task_num; i++ {
		tmp := make([]string, 1)
		tmp[0] = files[i]
		c.task_metas = append(c.task_metas, TaskMeta{
			task_id:       i,
			task_file:     tmp,
			intermediates: make([]string, 0),

			task_state:        idle,
			map_worker:        -1, // 表示没有worker正在处理这一任务
			map_start_time:    time.Now(),
			reduce_worker:     -1, // 表示没有worker正在处理这一任务
			reduce_start_time: time.Now(),
		})
		c.map_count[i] = false
	}
	c.task_queue = list.New()
	for i, meta := range c.task_metas {
		c.task_queue.PushBack(Task{
			task_id:       i,
			task_file:     meta.task_file,
			intermediates: make([]string, 0),
			task_operate:  to_map,
			task_worker:   -1, // 表示没有worker正在处理这一任务
			start_time:    time.Now(),
			NReduce:       nReduce,
		})
	}
	c.server()
	return &c
}
