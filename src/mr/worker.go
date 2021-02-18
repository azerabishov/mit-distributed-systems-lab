package mr


import "fmt"
import "log"
import "time"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


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
	mapOutput := make([]KeyValue, 0)
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
		
		kva := mapf(filename, string(content))
		mapOutput =  append(mapOutput, kva...)
	}()

	intermediates := make(map[string]struct{})
	for _, data := range mapOutput {
		func ()  {
			reduceID := ihash(data.Key) % nReduce;
			oname := fmt.Sprintf("mr-%v-%v",  taskID, reduceID)
			intermediates[oname] = struct{}{}

			ofile, err := os.OpenFile(oname,
				os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	
			if err != nil {
				log.Fatalln(err)
			}
					
			defer ofile.Close();
			fmt.Fprintf(ofile, "%v %v\n", data.Key, data.Value)
		}()
	}

	for key := range intermediates {
		intermediateFiles = append(intermediateFiles, key)
	}
	return 
}

func handleReduce(filenames []string, reduceID int, reducef func(string, []string) string)  {
	intermediate := make([]KeyValue, 0)
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

			intermediate = append(intermediate, KeyValue{Key: k, Value: v})
		}
	}



	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reduceID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()


	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
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
			handleReduce(filenames, reduceID, reducef)
			notifyMasterForFinishedTask([]string{}, taskID, taskType)
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