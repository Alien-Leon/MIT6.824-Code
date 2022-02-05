package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// 每个任务下发之后，worker上报其状态时，会独占任务的修改权
// 当任务执行超时、失败后，释放任务修改权，任务重新进入待执行队列中
type TaskAssigned struct {
	T Task
	*sync.Mutex
	AssignedKey  string
	AssignedTime time.Time
	ExpireAt     time.Time
}

func (t *TaskAssigned) Assign(key string, timeout time.Duration) {
	t.Lock()
	defer t.Unlock()

	t.AssignedTime = time.Now()
	t.ExpireAt = t.AssignedTime.Add(timeout)
	t.AssignedKey = key
}

func (t *TaskAssigned) ReleaseAssigned() {
	t.Lock()
	defer t.Unlock()

	t.releaseAssignedLocked()
}

func (t *TaskAssigned) releaseAssignedLocked() {
	log.Printf("task %v release assign to worker %v", t.T, t.AssignedKey)
	t.AssignedTime = time.Time{}
	t.ExpireAt = time.Time{}
	t.AssignedKey = ""
}

// Updatable 确认worker与任务的绑定关系，以及任务状态是否能正确流转
func (t *TaskAssigned) Updatable(key string) bool {
	t.Lock()
	defer t.Unlock()

	if t.T.Info().Status == "done" {
		return false
	}

	// 未绑定
	if t.AssignedKey == "" {
		return true
	}

	timeout := time.Since(t.ExpireAt) > 0
	if key != t.AssignedKey && !timeout {
		return false
	}

	return true
}


// Updatable 确认worker与任务的绑定关系，以及任务状态是否能正确流转
func (t *TaskAssigned) tryUpdateStatus(assignedKey string, status string) error {
	t.Lock()
	defer t.Unlock()

	// FIXME 这里的错误信息中，done by 没有值
	if t.T.Info().Status == "done" {
		return fmt.Errorf("%s unable update task status because task %v has already done by %s", assignedKey,t.T, t.AssignedKey)
	}

	// 已绑定但未超时
	timeout := time.Since(t.ExpireAt) > 0
	if  t.AssignedKey != "" && assignedKey != t.AssignedKey && !timeout {
		return fmt.Errorf("%s unable update task status because task %v belong to %s",assignedKey, t.T, t.AssignedKey)
	}

	t.T.Info().Status = status
	return nil
}

type TaskManager struct {
	Tasks           map[string]TaskAssigned // 任务只能在初始化Manager的时候添加
	
	Lock            *sync.Mutex
	TasksAssignedTo map[string]map[string]TaskAssigned // key=workerName, taskName

	doneCount int64
	taskCount int64
	Done      chan bool
	Queue     chan Task

	RunTimeout time.Duration
}

func NewTaskManger(runTimeout time.Duration) *TaskManager {
	m := &TaskManager{
		Done:       make(chan bool),
		Queue:      make(chan Task, 1000), // 最多只能承载1000任务的正常调度
		Tasks:      make(map[string]TaskAssigned),
		Lock: new(sync.Mutex),
		TasksAssignedTo: make(map[string]map[string]TaskAssigned),
		RunTimeout: runTimeout,
	}

	return m
}

func (m *TaskManager) AddTask(task Task) {
	m.Tasks[task.Info().Name] = TaskAssigned{T: task, Mutex: new(sync.Mutex)}
	m.taskCount++
}

func (m *TaskManager) Run() {
	// task enqueue for schedule
	for _, task := range m.Tasks {
		m.Queue <- task.T
	}

	go m.ScanTimeoutTask()

}

func (m *TaskManager) AssignTaskTo(task TaskAssigned, key string, timeout time.Duration) {
	task.Assign(key, m.RunTimeout)

	m.Lock.Lock()
	taskMap := m.TasksAssignedTo[key]
	if taskMap == nil {
		taskMap = make(map[string]TaskAssigned)
	}
	taskMap[task.T.Info().Name] = task
	m.TasksAssignedTo[key] = taskMap
	m.Lock.Unlock()
}

func (m *TaskManager) ReleaseAssigned(task TaskAssigned, key string) {
	task.ReleaseAssigned()

	m.Lock.Lock()
	tasksAssignToKey := m.TasksAssignedTo[key]
	delete(tasksAssignToKey, task.T.Info().Name)
	m.Lock.Unlock()

	log.Printf("release %s's assigned task %s", key, task.T)
}

func (m *TaskManager) UpdateTaskStatus(taskName string, status string, assignedKey string) error {
	task, ok := m.Tasks[taskName]
	if !ok {
		return fmt.Errorf("task %s not found", taskName)
	}

	err := task.tryUpdateStatus(assignedKey, status)
	if err != nil {
		return err
	}

	switch status {
	case "done":
		done := atomic.AddInt64(&m.doneCount, 1)
		if done == m.taskCount {
			close(m.Done)
		}
	case "running": 
		// TODO 如果多次上报的话，那么这里的任务绑定就需要调整
		m.AssignTaskTo(task, assignedKey, m.RunTimeout)
	case "error":
		log.Printf("task %v error. start reschedule\n", task)
		m.ReleaseAssigned(task, assignedKey)

		m.Queue <- task.T
	}

	return nil
}

func (m *TaskManager) ReAssignWorkerTask(workerName string) {
	var taskNeedEnqueue []TaskAssigned

	m.Lock.Lock()
	taskMap := m.TasksAssignedTo[workerName]
	for _, task := range taskMap {
		task.ReleaseAssigned()
		taskNeedEnqueue = append(taskNeedEnqueue, task)
	}

	m.Lock.Unlock()

	for _, task := range taskNeedEnqueue {
		m.Queue <- task.T
	}

}

func (m *TaskManager) ScanTimeoutTask() {
	for {
		time.Sleep(m.RunTimeout)

		var timeoutTasks []Task

		for _, task := range m.Tasks {
			task.Lock()
			info := task.T.Info()
			// FIXME 如果下面的是长时间的任务，那么这里的过期就会有问题

			if info.Status == "running" && time.Since(task.ExpireAt) > 0 {
				info.Status = "timeout"

				// TODO 这里也涉及到重调度
				// TODO 这里需要需要任务的所有权
				task.releaseAssignedLocked()
				timeoutTasks = append(timeoutTasks, task.T)
			}
			task.Unlock()
		}

		// FIXME  task MapTask-../pg-huckleberry_finn.txt timeout. start reschedule
		// FIXME 任务已经完成了，但是仍然被扫描出了超时，并且被重调度，这应该是因为临界区的问题
		// 在即将被扫描出超时的时候，任务做了上报done ？

		for _, task := range timeoutTasks {
			log.Printf("task %v timeout. start reschedule\n", task)
			m.Queue <- task
		}
	}
}
