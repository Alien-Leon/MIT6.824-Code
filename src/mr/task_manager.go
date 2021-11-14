package mr

import (
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

	t.releaseAssignedNoLock()
}

func (t *TaskAssigned) releaseAssignedNoLock() {
	t.AssignedTime = time.Time{}
	t.ExpireAt = time.Time{}
	t.AssignedKey = ""
}

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

	timeout := time.Now().Sub(t.ExpireAt) > 0
	if key != t.AssignedKey && !timeout {
		return false
	}

	return true
}

type TaskManager struct {
	Done chan bool // TODO 并发保护

	//Lock *sync.Mutex

	Tasks map[string]TaskAssigned // 任务只能在初始化Manager的时候添加
	// TODO 可优化为RWMap

	doneCount int64
	taskCount int64
	Queue     chan Task

	RunTimeout time.Duration // TODO 巡检超时任务，并做重新调度
}

func NewTaskManger(runTimeout time.Duration) *TaskManager {
	m := &TaskManager{
		Done:       make(chan bool),
		Queue:      make(chan Task, 1000),
		Tasks:      make(map[string]TaskAssigned),
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

func (m *TaskManager) UpdateTaskStatus(taskName string, status string, assignedKey string) {
	task, ok := m.Tasks[taskName]
	if !ok {
		return
	}

	if !task.Updatable(assignedKey) {
		return
	}

	if status == "done" {
		done := atomic.AddInt64(&m.doneCount, 1)
		if done == m.taskCount {
			close(m.Done)
		}

	} else if status == "running" {
		task.Assign(assignedKey, m.RunTimeout)
	}

	task.Lock()
	task.T.Info().Status = status
	task.Unlock()

	// 重调度
	if status == "error" {
		log.Printf("task %v error. start reschedule\n", task)

		task.ReleaseAssigned()
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
			if info.Status == "running" && time.Now().Sub(task.ExpireAt) > 0 {
				info.Status = "timeout"
				task.releaseAssignedNoLock()
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
