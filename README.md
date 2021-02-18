# MIT distributed systems lab1
This lab is a implementation of MapReduce system in golang. I implement a worker process that calls application Map and Reduce functions and handles reading and writing files, and a master process that hands out tasks to workers and copes with failed workers. I build something similar to  [the MapReduce paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).
## Usage
Through below command, you must build wc.go file:
`go build -race -buildmode=plugin ../mrapps/wc.go`

 In the main directory, run the master:
`go run -race mrcoordinator.go pg-*.txt`

 In one or more other windows, run some workers: 
`go run -race mrworker.go wc.so`
