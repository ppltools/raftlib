package main

import (
	"fmt"

	"time"

	"github.com/ppltools/raftlib"
)

func main() {
	r := raftlib.NewRaft(&raftlib.Config{
		Id:      1,
		Cluster: "http:127.0.0.1:2379",
	})
	r.Propose("stupig", "hello, world")
	// wait for log to be committed and applied
	time.Sleep(time.Second)
	fmt.Println(r.Lookup("stupig"))
	time.Sleep(time.Second * 5)
}
