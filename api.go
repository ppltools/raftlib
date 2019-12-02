package raftlib

import (
	"strings"

	"github.com/coreos/etcd/raft/raftpb"
)

type Config struct {
	Id         int
	Cluster    string
	Join       bool
	ServerPort int
}

type raftIns struct {
	kv          *Kvstore
	confChangeC chan raftpb.ConfChange
	errorC      <-chan error
	closed      bool
}

func (r *raftIns) Propose(key string, value string) {
	if r.Closed() {
		return
	}
	r.kv.Propose(key, value)
}

func (r *raftIns) Lookup(key string) (string, bool) {
	if r.Closed() {
		return "", false
	}
	return r.kv.Lookup(key)
}

func (r *raftIns) AddNode(nodeId uint64, url string) {
	if r.Closed() {
		return
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeId,
		Context: []byte(url),
	}
	r.confChangeC <- cc
}

func (r *raftIns) RemoveNode(nodeId uint64) {
	if r.Closed() {
		return
	}
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeId,
	}
	r.confChangeC <- cc
}

func (r *raftIns) IsLeader() bool {
	return false
}

func (r *raftIns) Closed() bool {
	if r.closed {
		return true
	}
	select {
	case <-r.errorC:
		r.closed = true
		return true
	default:
		return false
	}
}

func (r *raftIns) Close() {
	close(r.confChangeC)
	close(r.kv.proposeC)
}

type RaftApi interface {
	Propose(key, value string)
	Lookup(key string) (string, bool)

	AddNode(nodeId uint64, url string)
	RemoveNode(nodeId uint64)

	IsLeader() bool
}

func NewRaft(c *Config) RaftApi {
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)

	// raftIns provides a commit stream for the proposals from the http api
	var kvs *Kvstore
	getSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitC, errorC, snapshotterReady := NewRaftNode(c.Id, strings.Split(c.Cluster, ","), c.Join, getSnapshot, proposeC, confChangeC)

	kvs = NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	if c.ServerPort != 0 {
		// 非阻塞启动kv服务端，方便测试
		go func() {
			ServeHttpKVAPI(kvs, c.ServerPort, confChangeC, errorC)
		}()
	}

	return &raftIns{kv: kvs, confChangeC: confChangeC, errorC: errorC}
}
