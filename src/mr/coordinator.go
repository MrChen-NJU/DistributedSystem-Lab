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
	To_map TaskOperate = iota
	To_reduce
	To_exit
	To_wait
)

type TaskState uint32

// task状态
const (
	Idle TaskState = iota
	Mapping
	Reducing
	Done
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
	Task_id       int      // 任务id：0 ——> n-1
	Reduce_id     int      // reduce任务的桶号
	Task_file     []string // 该任务的原始文件名或reduce阶段的中间文件名
	Intermediates []string // map处理完成后的中间数据所在文件的文件名

	Task_state        TaskState // 当前任务的状态：idle\mmap\reduce\done
	Map_worker        int       // 执行map任务的worker
	Map_start_time    time.Time
	Reduce_worker     int // 执行reduce任务的worker
	Reduce_start_time time.Time
}

//
// 自定义的Task：使用Golang的RPC时结构体字段和函数名都要大写开头来表示可以被调用
//
type Task struct {
	Task_id       int      // 任务id：0 ——> n-1
	Task_file     []string // 该任务的文件名
	Intermediates []string // 该任务经过map处理后的中间文件的文件名

	Task_operate TaskOperate // 当前任务要执行的操作：to_map\to_reduce
	Task_worker  int         // 被指派当前任务的worker
	Start_time   time.Time   // 记录任务开始的时间

	Reduce_bucket int // 进行reduce时处理的桶号
	NReduce       int // reduce桶的个数
}

//
// 自定义的Coordinator
//
type Coordinator struct {
	// Your definitions here.
	NReduce int              // reduce桶的个数
	Phase   CoordinatorState // coordinator当前所处的状态：map\reduce\exit

	Task_metas         []TaskMeta  // 文件名与对应任务的映射
	Reduce_info        []TaskMeta  // 保存Reduce阶段任务的信息：此时的task_id为桶号
	Task_queue         *list.List  // 任务队列
	Map_task_worker    map[int]int // 执行的map任务号与worker id的映射
	Reduce_task_worker map[int]int // 执行的reduce任务号与worker id的映射

	Map_total    int    // 总的需要map处理的个数，等于待处理的文件的个数
	Map_count    []bool // 记录已完成的map任务
	Reduce_total int    // 总的需要reduce处理的个数，等于NReduce
	Reduce_count []bool // 记录已完成的reduce任务
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
// 在coordinator中另起一个线程每5秒检查一次Task_metas看是否有任务超时
//
func (c *Coordinator) checkCrashWorker() {
	for {
		time.Sleep(5 * time.Second)
		mutex.Lock()
		switch c.Phase {
		case MAP:
			for i, meta := range c.Task_metas {
				if meta.Task_state == Mapping && (time.Since(meta.Map_start_time)) > 10*time.Second {
					c.Task_queue.PushBack(Task{
						Task_id:       meta.Task_id,
						Task_file:     meta.Task_file,
						Intermediates: make([]string, 0),
						Task_operate:  To_map,
						Task_worker:   -1, // 表示没有worker正在处理这一任务
						Start_time:    time.Now(),
						NReduce:       c.NReduce,
					})
					c.Task_metas[i].Task_state = Idle
				}
			}
		case REDUCE:
			for i, info := range c.Reduce_info {
				if info.Task_state == Reducing && (time.Since(info.Reduce_start_time)) > 10*time.Second {
					c.Task_queue.PushBack(Task{
						Task_id:       info.Task_id,
						Task_file:     info.Task_file,
						Intermediates: make([]string, 0),
						Task_operate:  To_reduce,
						Task_worker:   -1, // 表示没有worker正在处理这一任务
						Start_time:    time.Now(),
						Reduce_bucket: info.Reduce_id,
						NReduce:       c.NReduce,
					})
					c.Reduce_info[i].Task_state = Idle
				}
			}
		case EXIT:
			mutex.Unlock()
			return
		}
		mutex.Unlock()
	}
}

//
// 被worker通过RPC调用来向coordinator请求任务
//
func (c *Coordinator) AssignTask(args *int, reply *Task) error {
	mutex.Lock()
	defer mutex.Unlock()
	worker_id := *args
	if c.Phase != EXIT && c.Task_queue.Len() > 0 {
		// fmt.Printf("=== Assign task length: %d, Phase: %d\n", c.Task_queue.Len(), c.Phase)
		if c.Phase == MAP && c.Task_queue.Front().Value.(Task).Task_operate == To_map {
			// coordinator阶段为MAP且任务队列里的任务为map任务
			*reply = (c.Task_queue.Front().Value.(Task)) // 初始化reply
			tid := (*reply).Task_id
			c.Task_queue.Remove(c.Task_queue.Front())
			c.Map_task_worker[tid] = worker_id // 建立任务id和worker id的映射

			c.Task_metas[tid].Task_state = Mapping
			c.Task_metas[tid].Map_worker = worker_id
			c.Task_metas[tid].Map_start_time = time.Now()

			reply.Task_worker = worker_id
			reply.Start_time = c.Task_metas[tid].Map_start_time

		} else if c.Phase == REDUCE && c.Task_queue.Front().Value.(Task).Task_operate == To_reduce {
			// coordinator阶段为REDUCE且任务队列里的任务为reduce任务
			*reply = (c.Task_queue.Front().Value.(Task)) // 初始化reply
			tid := (*reply).Task_id
			c.Task_queue.Remove(c.Task_queue.Front())
			c.Reduce_task_worker[tid] = worker_id // 建立任务id和worker id的映射

			c.Reduce_info[tid].Task_state = Reducing
			c.Reduce_info[tid].Reduce_worker = worker_id
			c.Reduce_info[tid].Reduce_start_time = time.Now()

			reply.Task_worker = worker_id
			reply.Start_time = c.Reduce_info[tid].Reduce_start_time
		}
	} else if c.Phase == EXIT {
		*reply = Task{
			Task_operate: To_exit,
		}
	} else {
		*reply = Task{
			Task_operate: To_wait,
		}
	}
	return nil
}

//
// 被worker通过RPC调用来通知coordinator任务完成
//
func (c *Coordinator) ComplateTask(args *Task, reply *ExampleReply) error {
	mutex.Lock()
	defer mutex.Unlock()
	task_id := args.Task_id
	switch args.Task_operate {
	case To_map:
		if !c.Map_count[task_id] {
			copy(c.Task_metas[task_id].Intermediates, args.Intermediates)
			delete(c.Map_task_worker, task_id)
			c.Map_count[task_id] = true
			c.Task_metas[task_id].Task_state = Done
			if check_all_true(c.Map_count) {
				// 所有map任务执行完成后，coordinator进入REDUCE阶段：准备nReduce个reduce任务
				c.Phase = REDUCE
				fmt.Println("---- switch to reduce now... ----")
				for i := 0; i < c.NReduce; i++ {
					intermediate_file_names := make([]string, 0)
					for j := 0; j < c.Map_total; j++ {
						intermediate_file_names = append(intermediate_file_names, fmt.Sprintf("mr-inter-%d-%d.json", j, i))
					}
					c.Task_queue.PushBack(Task{
						Task_id:       i,
						Task_file:     intermediate_file_names,
						Task_operate:  To_reduce,
						Task_worker:   -1,
						Start_time:    time.Now(),
						Reduce_bucket: i,
						NReduce:       c.NReduce,
					})
					c.Reduce_info = append(c.Reduce_info, TaskMeta{
						Task_id:           i, // 在这里的task_id和桶号相对应
						Reduce_id:         i,
						Task_file:         intermediate_file_names,
						Task_state:        Idle,
						Reduce_worker:     -1,
						Reduce_start_time: time.Now(),
					})
				}
			}
		}
	case To_reduce:
		bucket_id := args.Reduce_bucket
		if !c.Reduce_count[bucket_id] {
			delete(c.Reduce_task_worker, task_id)
			c.Reduce_count[task_id] = true
			c.Reduce_info[task_id].Task_state = Done
			if check_all_true(c.Reduce_count) {
				// 所有reduce任务执行完成后，coordinator进入EXIT阶段
				fmt.Println("*** all map and reduce tasks finished ***")
				c.Phase = EXIT
				for i := range c.Task_metas {
					c.Task_metas[i].Task_state = Done
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
	go http.Serve(l, nil) // 起一个goroutine监听RPC请求
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mutex.Lock()
	defer mutex.Unlock()
	ret := c.Phase == EXIT
	if ret {
		time.Sleep(3 * time.Second)
		//fmt.Println("The program is finished, exiting... :)")
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
	task_num := len(files)
	c.NReduce = nReduce
	c.Phase = MAP
	fmt.Println("---- switch to map now... ----")
	c.Map_count = make([]bool, task_num)
	c.Map_total = task_num
	c.Reduce_count = make([]bool, nReduce)
	c.Map_task_worker = make(map[int]int, 0)
	c.Reduce_task_worker = make(map[int]int, 0)
	for i := 0; i < nReduce; i++ {
		c.Reduce_count[i] = false
	}
	c.Reduce_total = nReduce
	for i := 0; i < task_num; i++ {
		c.Task_metas = append(c.Task_metas, TaskMeta{
			Task_id:       i,
			Task_file:     []string{files[i]},
			Intermediates: make([]string, 0),

			Task_state:        Idle,
			Map_worker:        -1, // 表示没有worker正在处理这一任务
			Map_start_time:    time.Now(),
			Reduce_worker:     -1, // 表示没有worker正在处理这一任务
			Reduce_start_time: time.Now(),
		})
		c.Map_count[i] = false
	}
	c.Task_queue = list.New()
	for i, meta := range c.Task_metas {
		c.Task_queue.PushBack(Task{
			Task_id:       i,
			Task_file:     meta.Task_file,
			Intermediates: make([]string, 0),
			Task_operate:  To_map,
			Task_worker:   -1, // 表示没有worker正在处理这一任务
			Start_time:    time.Now(),
			NReduce:       nReduce,
		})
	}
	c.server()
	go c.checkCrashWorker()
	return &c
}
