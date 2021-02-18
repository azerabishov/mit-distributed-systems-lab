package mr

import "strconv"
import "os"

type Args struct {

}


type Reply struct {
	TaskID 		int
	TaskType	int
	Filenames 	[]string
	NReduce 	int
	ReduceID	int
}



type NotifyMasterArg struct {
	TaskID				int
	TaskType			int
	IntermediateFiles	[]string
}


type NotifyMasterReply struct {
}


func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
