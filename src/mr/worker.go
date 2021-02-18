package mr


import "fmt"
import "log"
import "time"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func handleMap(taskID int, filename string, nReduce int, mapf func(string, string) []KeyValue) (intermediateFiles []string) {
	kva := make([]KeyValue, 0)

	func(){
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatal("dialing error:", err)
		}
		
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("dialing error:", err)
		}
		
		kva = mapf(filename, string(content))
	}()

	intermediateFiles = make([]string, 0)
	for _, data := range kva {
		func ()  {
			reduceID := ihash(data.Key) % nReduce;
			oname := fmt.Sprintf("mr-%v-%v",  taskID, reduceID)
			ofile, err := os.OpenFile(oname,
				os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	
			if err != nil {
				log.Fatalln(err)
			}
					
			defer ofile.Close();
			fmt.Fprintf(ofile, "%v %v\n", data.Key, data.Value)
			intermediateFiles = append(intermediateFiles, oname)
		}()
	}
	return 
}

func handleReduce(filenames []string, reduceID int)  {
	kva := make([]KeyValue, 0)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatal("dialing error:", err)
		}

		for {
			k := ""
			v := ""
			nWord, err := fmt.Fscanf(file, "%s %s", &k, &v)

			if nWord != 2 || err != nil {
				break
			}

			kva = append(kva, KeyValue{Key: k, Value: v})
		}
	}

	oname := fmt.Sprintf("mr-out-%v" , reduceID)

	for _, data := range kva {
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		fmt.Fprintf(ofile, "%v %v\n", data.Key, data.Value)
	}
}	


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	for {
		taskID, taskType, filenames, nReduce, reduceID := GetTask()
		fmt.Println("taskID:", taskID, "taskType:", taskType, "filenames: ", filenames, "reduceID: ", reduceID)
		if taskType == Map {
			intermediateFiles := handleMap(taskID, filenames[0], nReduce, mapf)
			notifyMasterForFinishedTask(intermediateFiles, taskID, taskType)
		} else if taskType == Reduce {
			handleReduce(filenames, reduceID)
		}else if taskType == NoTask {
			time.Sleep(time.Second)
		}
	}

}

func notifyMasterForFinishedTask(filenames []string, taskID int, taskType int) {
	args := NotifyMasterArg{}
	reply := NotifyMasterReply{}
	args.IntermediateFiles = filenames
	args.TaskType = taskType
	args.TaskID = taskID
	call("Master.HandleFinishedTask", &args, &reply)
}

func GetTask()  (taskID int, taskType int, filenames []string, nReduce int, reduceID int){
	args := Args{}
	reply := Reply{}

	call("Master.SendTask", &args, &reply)
	taskID = reply.TaskID
	taskType = reply.TaskType
	filenames = reply.Filenames
	nReduce = reply.NReduce
	reduceID = reply.ReduceID
	return
}



func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock()
	
	client, err := rpc.DialHTTP("unix", sockname)

    if err != nil {
        log.Fatal("dialing error:", err)
    }

	err = client.Call(rpcname, args, reply)

	return false
}