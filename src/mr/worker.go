package mr

import (
	"context"
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	c1 := context.WithValue(context.Background(), "mapf", mapf)
	c2 := context.WithValue(c1, "reducef", reducef)
	ctx, fn := context.WithCancel(c2)
	w := &worker{
		Name:     fmt.Sprintf("worker-%v", os.Getpid()),
		ctx:      ctx,
		cancelFn: fn,
	}

	for !w.register() {
		log.Printf("%v register fail. retrying...\n", w.Name)
		time.Sleep(time.Second)
	}

	go w.HeartBeat()

	for {
		task := w.getTask()
		log.Printf("worker %v get task %v", w.Name, task)
		// TODO 任务状态流转与上报

		// 当前上报只有一次，不会在任务运行时做周期性上报，一旦上报请求失败，那么coordinator的任务状态更新也会失败，随后任务可能会被重新调度执行
		w.reportTaskStatus(task, "running")
		err := task.Do(w.ctx, w)
		if err != nil {
			log.Printf("worker %v do task %v error. %v", w.Name, task, err)
			w.reportTaskStatus(task, "error")
			time.Sleep(time.Second)

			continue
		}

		w.reportTaskStatus(task, "done")
	}

}

type worker struct {
	Name string

	ctx      context.Context
	cancelFn context.CancelFunc
}

/* TODO  文件的原子性，避免中途崩溃
To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. You can use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.
*/

func (w *worker) register() (registered bool) {
	call("Coordinator.RegisterWorker", w.Name, &registered)
	return
}

func (w *worker) offline() (ok bool) {
	w.cancelFn()

	call("Coordinator.OfflineWorker", w.Name, &ok)
	return
}

func (w *worker) getTask() (t Task) {
	call("Coordinator.GetTask", w.Name, &t)
	return
}

func (w *worker) reportTaskStatus(t Task, status string) bool {
	if _, ok := t.(*ControlTask); ok {
		return true
	}

	return call("Coordinator.ReportTaskStatus",
		ReportTaskStatusReq{Status: status, T: t, WorkerName: w.Name},
		&struct{}{})
}

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

	log.Println(rpcname, err)
	return false
}

func (w *worker) HeartBeat() {
	for {
		time.Sleep(time.Second)

		select {
		case <-w.ctx.Done():
			return
		default:
			if !call("Coordinator.HeartBeatWorker", w.Name, &struct{}{}) {
				// 心跳失败/超时时，尝试重新注册，在coordinator处恢复工作状态
				log.Printf("worker %v heartbeat fail. try register again", w.Name)

				w.register()
			}
		}
	}
}
