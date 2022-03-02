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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
// 通过rpc调用获取任务
//
func getTask() Task {
	task := Task{}
	id := os.Getpid()
	call("Coordinator.AssignTask", &id, &task)
	return task
}

//
// 通过rpc调用通知任务完成
//
func completeTask(paths []string, task *Task) {
	copy(task.intermediates, paths)
	call("Coordinator.CompleteTask", &ExampleArgs{}, &task)
}

//
// 使用mapf、reducef、相应的task信息执行任务
//
func excuteTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task Task) {
	switch task.task_operate {
	case to_map:
		// 打开任务文件并读取进行map处理
		filename := task.task_file[0] // 在map阶段暂时只支持一个任务中有一个输入文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kvlist := mapf(filename, string(content))

		// 将map输出的中间结果根据哈希值进行划分
		buffer := make([][]KeyValue, task.NReduce)
		for _, kv_pair := range kvlist {
			slot := ihash(kv_pair.Key) % task.NReduce
			buffer[slot] = append(buffer[slot], kv_pair)
		}

		intermediates := make([]string, 0)
		// 创建"mr-out-task_id-slot.json"文件并写入
		for i := 0; i <= task.NReduce; i++ {
			output_path := fmt.Sprintf("mr-out-%d-%d.json", task.task_id, i)
			filePtr, err := os.Create(output_path)
			if err != nil {
				fmt.Println("文件创建失败", err.Error())
				return
			}
			defer filePtr.Close()
			// 创建Json编码器
			encoder := json.NewEncoder(filePtr)
			for _, kv := range buffer[i] {
				err = encoder.Encode(&kv)
				if err != nil {
					fmt.Println("写入json文件失败", err.Error())
					return
				}
			}
			intermediates = append(intermediates, output_path)
		}
		completeTask(intermediates, &task)

	case to_reduce:
		// 从bucket中的所有中间文件中取出所有keyvalue对
		filenames := task.task_file
		kvlist := make([]KeyValue, 0)
		for _, filename := range filenames {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvlist = append(kvlist, kv)
			}
			file.Close()
		}
		sort.Sort(ByKey(kvlist))
		i := 0
		output_filename := fmt.Sprintf("mr-out-%d.txt", task.reduce_bucket)
		output_file, err := os.Open(output_filename)
		if err != nil {
			log.Fatalf("cannot open %v", output_filename)
		}
		// 输出到mr-out-bucket_id.txt
		for i < len(kvlist) {
			j := i + 1
			for j < len(kvlist) && kvlist[j].Key == kvlist[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvlist[k].Value)
			}
			output := reducef(kvlist[i].Key, values)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(output_file, "%v %v\n", kvlist[i].Key, output)
			i = j
		}
		output_file.Close()
		tmp := []string{}
		tmp = append(tmp, output_filename)
		completeTask(tmp, &task)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	task := Task{
		task_id: -1,
	}
	for i := 0; i >= 0; i++ {
		task = getTask()
		if task.task_id != -1 {
			excuteTask(mapf, reducef, task)
		} else {
			time.Sleep(time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
