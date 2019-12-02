package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ppltools/raftlib"
)

func main() {
	r, err := raftlib.NewRaft(&raftlib.Config{
		Cluster:     "http://172.24.25.75:2379",
		PersistRoot: "data/member1/",
		ServerPort:  8080,
	})
	if err != nil {
		fmt.Printf("new raft instance failed: %s\n", err)
		os.Exit(-1)
	}
	r.Propose("stupig", "hello, world")
	// wait for log to be committed and applied
	time.Sleep(time.Second)
	fmt.Println(r.Lookup("stupig"))
	time.Sleep(time.Second * 10)
	fmt.Println(r.Lookup("stupig"))

	for true {
		time.Sleep(time.Second * 4)
		fmt.Println(r.IsLeader())

		if r.Closed() {
			r.Close()
		}
	}
}
