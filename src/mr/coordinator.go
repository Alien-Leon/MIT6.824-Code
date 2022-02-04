package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"

	"time"
	"go.uber.org/atomic"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	workers sync.Map // map[string]*WorkerInstance

	files      []string
	nReduce    int
	done       chan bool
	mapTask    *TaskManager
	reduceTask *TaskManager
}

type WorkerInstance struct {
	Name          string
	HeartBeatTime atomic.Time // TODO 如果太久没有心跳，那么认为实例已离线，任务需要重新分发

	// 如果任务因超时而离线，后续任务完成时又进行了上报，那么此时需要保证上报任务状态的原子性以及幂等性。
	// 同时需要确认任务的归属，保证任务状态不会被其他worker修改
}

// 任务调度
// 执行任务时，定期输出任务总进度 - 维护任务全局信息
// 简单模式下，所有map任务完成之后，统一执行reduce任务。更复杂一些时，map和reduce任务可以根据调度需求做同时执行，提高效率。

// WorkerInstance 注册与心跳管理
// WorkerInstance 上报任务状态、进度
// WorkerInstance 从待执行任务队列中获取任务信息

func (c *Coordinator) RegisterWorker(name string, registered *bool) error {
	worker := &WorkerInstance{Name: name}
	_, loaded := c.workers.LoadOrStore(worker.Name, worker)
	if loaded {
		return fmt.Errorf("worker %v register conflict", name)
	}

	*registered = true
	log.Printf("worker %v has registered.\n", worker.Name)
	return nil
}

func (c *Coordinator) OfflineWorker(name string, ok *bool) error {
	c.workers.Delete(name)
	// TODO 确认离线时关联的任务状态？-> 简单处理的话，可以等自然超时
	*ok = true
	log.Printf("worker %v offline.\n", name)
	return nil
}

func (c *Coordinator) HeartBeatWorker(name string, _ *struct{}) error {
	v, ok := c.workers.Load(name)
	if !ok {
		return fmt.Errorf("worker has not registered")
	}
	w := v.(*WorkerInstance)
	w.HeartBeatTime.Store(time.Now())
	return nil
}

func (c *Coordinator) ReportTaskStatus(req ReportTaskStatusReq, _ *struct{}) error {
	// TODO 按理说这边是需要校验worker是否正确的，有可能存在前一个worker上报超时，
	// 而后一个worker已经在运行中，那么两边的任务状态上报会冲突
	switch req.T.Info().Type {
	case "MapTask":
		c.mapTask.UpdateTaskStatus(req.T.Info().Name, req.Status, req.WorkerName)
	case "ReduceTask":
		c.reduceTask.UpdateTaskStatus(req.T.Info().Name, req.Status, req.WorkerName)
	}

	log.Printf("worker %s report %v -> %v ", req.WorkerName, req.T, req.Status)

	return nil
}

func (c *Coordinator) GetTask(name string, t *Task) error {
	select {
	case task := <-c.mapTask.Queue:
		*t = task
	case task := <-c.reduceTask.Queue:
		*t = task
	case <-c.done:
		task := Task(&ControlTask{TaskInfo{Type: "Exit"}})
		*t = task
	default:
		task := Task(&ControlTask{TaskInfo{Type: "Wait"}})
		*t = task
	}

	//log.Printf("worker %v get task %v", name, *t)
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
	<-c.done

	return true
}

func (c *Coordinator) checkWorkerHealth() {
	healthTimeout := time.Second * 5

	for {
		now := time.Now()
		
		var workersUnhealthy []*WorkerInstance
		c.workers.Range(func(key, value interface{}) bool {
			w := value.(*WorkerInstance)
			if now.Sub(w.HeartBeatTime.Load()) > healthTimeout {
				c.mapTask.ReAssignWorkerTask(w.Name)
				c.reduceTask.ReAssignWorkerTask(w.Name)
				workersUnhealthy = append(workersUnhealthy, w)
			}

			return true
		})

		for _, worker := range workersUnhealthy {
			c.workers.Delete(worker.Name)
			log.Printf("worker %s is unhealthy, now cleaning.", worker.Name)
		}

		time.Sleep(time.Second)
	}
}

func (c *Coordinator) coordinate() {
	c.dispatchMapTask()
	go c.mapTask.Run()

	<-c.mapTask.Done

	log.Println("map task done")

	c.dispatchReduceTask()
	go c.reduceTask.Run()
	<-c.reduceTask.Done

	log.Println("reduce task done")
	log.Println("all task done")

	close(c.done)
}

func (c *Coordinator) dispatchMapTask() {
	// 将map任务分发之前，需要对大文件进行切分，切分成足够小的文件使得任务易于分发。
	// 在GFS中，文件的粒度为64KB，在本地测试中，文件粒度已经较小，因此不必再做预切分
	for _, f := range c.files {
		task := &MapTask{
			TaskInfo: TaskInfo{
				Name:   f,
				Type:   "MapTask",
				Status: "Waiting",
			},
			NReduce:       c.nReduce,
			InputFilePath: f,
		}
		c.mapTask.AddTask(task)
	}
}

func (c *Coordinator) dispatchReduceTask() {
	fileInfo, err := ioutil.ReadDir("./")
	if err != nil {
		log.Printf("read intermediate file error. %v", err)
		return
	}

	for _, file := range fileInfo {
		if strings.HasPrefix(file.Name(), "mr-intermediate") {
			task := &ReduceTask{
				TaskInfo: TaskInfo{
					Name:   file.Name(),
					Type:   "ReduceTask",
					Status: "Waiting",
				},
			}
			c.reduceTask.AddTask(task)
		}
	}

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		done:       make(chan bool),
		mapTask:    NewTaskManger(time.Minute),
		reduceTask: NewTaskManger(time.Minute),
	}

	// 任务队列 map - wait - reduce - wait - done

	// map task、reduce task 以及 pseudo task 性质不一样，一类是独占的、一类是需要共享的

	// MR 框架中，对应单主多从架构，主会成为单点问题
	// 当主挂掉之后，所有从节点都不再工作，这样的流程是十分简单的，但可用性与效率并不好
	// 所有的worker在整体任务结束前，都需要等待直至任务完成后才可退出，以防有失败的任务需要重新执行
	go c.coordinate()

	c.server()
	return &c
}
