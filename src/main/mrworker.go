package main



import (
	"../mr"
	"fmt"
	"os"
	"log"
	"plugin"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	mr.Worker(mapf, reducef)
}


func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}

	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("Cannot find map in %v", filename)
	}

	mapf := xmapf.(func(string, string) []mr.KeyValue)

	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}

	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef

}