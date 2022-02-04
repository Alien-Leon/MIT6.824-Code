package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"bufio"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)
import "strconv"

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// 日志的处理可以更通用些
	log.SetOutput(os.Stdout)

	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	gob.Register(&ControlTask{})
}

type TaskInfo struct {
	Type      string
	Name      string
	Status    string
	StartTime time.Time
	EndTime   time.Time
	Worker    *WorkerInstance
}

func (t *TaskInfo) String() string {
	return fmt.Sprintf("%v-%v", t.Type, t.Name)
}

func (t *TaskInfo) Info() *TaskInfo {
	return t
}

type Task interface {
	fmt.Stringer
	Info() *TaskInfo
	Do(ctx context.Context, w *worker) error
}

type ControlTask struct {
	TaskInfo
}

func (t *ControlTask) Do(ctx context.Context, w *worker) error {
	switch t.Type {
	case "Exit":
		log.Println("All tasks done. exit...")
		w.offline()
		os.Exit(0)
	case "Wait":
		log.Println("Waiting for task")
		time.Sleep(time.Second)
	}
	return nil
}

type MapTask struct {
	TaskInfo
	NReduce       int
	InputFilePath string
}

func (t *MapTask) Do(ctx context.Context, w *worker) error {
	mapFn := ctx.Value("mapf").(func(string, string) []KeyValue)
	f, err := ioutil.ReadFile(t.InputFilePath)
	if err != nil {
		return fmt.Errorf("task %v fail. %w", t, err)
	}

	// 生成中间文件的时候，是否会跟其他worker共享写入一个文件?即直接进行shuffle环节
	kvs := mapFn(t.InputFilePath, string(f))
	sort.Sort(ByKey(kvs))

	m := map[int][]KeyValue{}
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		m[idx] = append(m[idx], kv)
	}

	for idx, kvs := range m {
		// mr-{MapTaskNumber}-{ReduceTaskNumber} 任务提示可用这种形式的文件名存储中间kv，但这样的话shuffle过程就需要额外多的一步了
		filename := fmt.Sprintf("mr-intermediate-%v", idx)
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		for _, kv := range kvs {
			if _, err = f.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value)); err != nil {
				return err
			}
		}

		_ = f.Close()
	}

	return nil
}

type ReduceTask struct {
	TaskInfo
}

func (t *ReduceTask) Do(ctx context.Context, w *worker) error {
	kvMap := map[string][]string{}

	f, err := os.Open(t.Name)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		kvs := strings.Fields(line)
		if len(kvs) != 2 {
			return fmt.Errorf("line '%s' format incorrect. ", line)
		}

		k := kvs[0]
		kvMap[k] = append(kvMap[k], kvs[1])
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	idx := t.Name[strings.LastIndex(t.Name, "-")+1:]
	filename := fmt.Sprintf("mr-out-%v", idx)
	f, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	reducef := ctx.Value("reducef").(func(string, []string) string)
	for k, v := range kvMap {
		out := reducef(k, v)
		if _, err = f.WriteString(fmt.Sprintf("%v %v\n", k, out)); err != nil {
			return err
		}
	}

	_ = f.Close()

	return nil
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type ReportTaskStatusReq struct {
	T          Task
	Status     string
	WorkerName string
}
