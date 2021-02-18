package mr

import (
	"fmt"
	"time"
	"net/rpc"
	"net/http"
	"net"
	"log"
	"os"
	"sync"
	"strings"
	"strconv"
)


type Task struct {
	taskID 		  int
	taskType      int
	taskState	  int
	startTime     time.Time
	filenames	  []string
	reduceID	  int
}


type Master struct {
	nReduce 				int
	nMap 					int
	nextWorkerID 			int
	nextTaskID  			int
	tasks 					map[int]Task
	tasksLock 				sync.Mutex
	finishedMapOutputFiles  []string
	finishedMapTask 		int
	finishedMapTaskLock 	sync.Mutex
	finishedReduceTask		int
	finishedReduceTaskLock	sync.Mutex
}

func (m *Master) NextTaskID() (id int) {
	id = m.nextTaskID
	m.nextTaskID++
	return
}


func (m *Master) HandleFinishedTask(args *NotifyMasterArg, reply *NotifyMasterReply) error {
	m.tasksLock.Lock()
	defer m.tasksLock.Unlock()

	task := m.tasks[args.TaskID];
	if task.taskState == Finished {
		return nil
	}
	task.taskState = Finished
	m.tasks[args.TaskID] = task;
	
	if args.TaskType == Map {
		m.finishedMapOutputFiles = append(m.finishedMapOutputFiles, (args.IntermediateFiles)...)
		m.finishedMapTask++
	}else if args.TaskType == Reduce {
		m.finishedReduceTask++
	}
	return nil
}


func (m *Master) SendTask(args *Args, reply *Reply) error {

	reply.TaskType = NoTask


	m.tasksLock.Lock()
	defer m.tasksLock.Unlock()
	

	for _, task := range m.tasks {
		if task.taskState != Idle {
			continue
		}
		task.taskState = Working;
		task.startTime = time.Now();
		m.tasks[task.taskID] = task;

		*reply = Reply{
			TaskID: task.taskID,
			TaskType: task.taskType,
			Filenames: task.filenames,
			NReduce: m.nReduce,
			ReduceID: task.reduceID,
		}

		break
	}
	
	return nil
}

func (m *Master) Done() bool {
	// Your code here.
	m.tasksLock.Lock()
	defer m.tasksLock.Unlock()
	return m.finishedReduceTask == m.nReduce
}


func (m * Master) server()  {
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



func MakeMaster(files []string, nReduce int)  *Master{
	fmt.Println()

	finishedMapOutputFiles := make([]string, 0);

	m := Master{
		nReduce: 		nReduce,
		nextWorkerID:	0,
		nextTaskID:		0,
		nMap:			0,
		tasks:          make(map[int]Task),
		finishedMapOutputFiles: finishedMapOutputFiles,
	}

	var task Task
	for _, filename := range files {
		nextTaskID := m.NextTaskID()
		task = Task{
			taskID:		nextTaskID,
			taskType:	Map,
			taskState:  Idle,
			filenames:	[]string{filename},
			startTime:	time.Time{},
			reduceID:   -1,
		}
		m.tasks[task.taskID] = task
		m.nMap ++
	}

	go m.handleTimeoutTasks()
	go m.mergeIntermediateFiles()

	m.server()
	
	return &m
}



func (m *Master) handleTimeoutTasks()  {
	for {
		func(){
			m.tasksLock.Lock()
			defer m.tasksLock.Unlock()

			for i := range m.tasks {

				if m.tasks[i].taskState != Working {
					continue
				}
		
				if time.Now().Sub(m.tasks[i].startTime) < 10 * time.Second {
					continue
				}

				task := m.tasks[i]
				task.taskState = Idle
				m.tasks[i] = task
			}
		}()

		time.Sleep(time.Second)
	}

}

func (m *Master) mergeIntermediateFiles()  {
	for {
		isMapTaskFinished := func() bool{
			m.tasksLock.Lock()
			defer m.tasksLock.Unlock()
			return m.nMap == m.finishedMapTask	
		}()

		if(isMapTaskFinished){
			break
		}

		time.Sleep(time.Second)
	}

	fmt.Println("-------------------------------------")
	fmt.Println(m.finishedMapOutputFiles)

	spliter := func(r rune) bool { return r == '-' }
	reduceFiles := make(map[int][]string)
	for _, filename := range m.finishedMapOutputFiles {
		fields := strings.FieldsFunc(filename, spliter)
		reduceID, err := strconv.Atoi(fields[2])
		if err != nil {
			log.Fatal("listen error:", err)
		}
		reduceFiles[reduceID] = append(reduceFiles[reduceID], filename)
	}

	m.tasksLock.Lock()
	defer m.tasksLock.Unlock()

	var task Task
	for i := 0; i < m.nReduce; i++ {
		nextTaskID := m.NextTaskID()
		task = Task{
			taskID:		nextTaskID,
			taskType:	Reduce,
			taskState:  Idle,
			filenames:	reduceFiles[i],
			startTime:	time.Time{},
			reduceID:   i,
		}
		m.tasks[task.taskID] = task
		m.nReduce ++
	}
	
}