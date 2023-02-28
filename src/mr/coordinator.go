package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type TaskStatus int
type State int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

const TIMEOUT = time.Second * 10

type Coordinator struct {
	// Your definitions here.
	Files   []string
	NReduce int
	Mutex   sync.Mutex

	State         State
	TaskQueue     chan *Task
	TaskMeta      []*Task
	Intermediates [][]string
}

type Task struct {
	ID            int
	File          string
	StartTime     time.Time
	Status        TaskStatus
	Type          State
	NReduce       int
	Intermediates []string
}

func (c *Coordinator) AllocTask(args *EmptyArgs, reply *Task) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if len(c.TaskQueue) > 0 {
		task := <-c.TaskQueue
		task.Status = InProgress
		task.StartTime = time.Now()
		*reply = *task
	} else if c.State == Exit {
		*reply = Task{Type: Exit}
	} else {
		*reply = Task{Type: Wait}
	}

	return nil
}

func (c *Coordinator) CrashTask(id *int, reply *EmptyReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	task := c.TaskMeta[*id]
	task.Status = Idle

	return nil
}

func (c *Coordinator) CompleteTask(task *Task, reply *EmptyReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.TaskMeta[task.ID] = task
	if task.Status == Completed {
		return nil
	}
	task.Status = Completed

	go c.processTask(task)

	return nil
}

func (c *Coordinator) processTask(task *Task) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	switch task.Type {
	case Map:
		for reduceTaskId, file := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], file)
		}
		if c.checkTaskDone() {
			c.State = Reduce
			c.createReduceTask()
		}
	case Reduce:
		if c.checkTaskDone() {
			c.State = Exit
		}
	}
}

func (c *Coordinator) checkTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkTaskTimeout() {
	for {
		time.Sleep(time.Second * 5)
		c.Mutex.Lock()
		if c.State == Exit {
			c.Mutex.Unlock()
			return
		}

		for _, task := range c.TaskMeta {
			if task.Status == InProgress && time.Since(task.StartTime) > TIMEOUT {
				task.Status = Idle
				c.TaskQueue <- task
			}
		}
		c.Mutex.Unlock()
	}
}

func (c *Coordinator) createMapTask() {
	for id, file := range c.Files {
		task := Task{
			ID:      id,
			File:    file,
			Status:  Idle,
			Type:    Map,
			NReduce: c.NReduce,
		}
		c.TaskQueue <- &task
		c.TaskMeta = append(c.TaskMeta, &task)
	}
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make([]*Task, 0)
	for id, files := range c.Intermediates {
		task := Task{
			ID:            id,
			Intermediates: files,
			Type:          Reduce,
			Status:        Idle,
			NReduce:       c.NReduce,
		}
		c.TaskQueue <- &task
		c.TaskMeta = append(c.TaskMeta, &task)
	}
}

func (c *Coordinator) createFolder() {
	dir, _ := os.Getwd()
	intermediate := filepath.Join(dir, "intermediate")
	output := filepath.Join(dir, "output")
	os.Mkdir(intermediate, os.ModePerm)
	os.Mkdir(output, os.ModePerm)
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.State == Exit

}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:         files,
		NReduce:       nReduce,
		State:         Map,
		TaskQueue:     make(chan *Task, max(len(files), nReduce)),
		TaskMeta:      make([]*Task, 0),
		Intermediates: make([][]string, nReduce),
	}

	c.createMapTask()
	c.server()
	go c.checkTaskTimeout()

	return &c
}
