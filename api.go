package raftlib

import (
	"errors"
	"strings"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

type Config struct {
	Id          int    // node id，如果用户未指定，根据本机IP扫描cluster
	Cluster     string // 集群地址
	Join        bool   // 是否加入新集群
	ServerPort  int    // 用户端口，类似于etcd 2379
	PersistRoot string // 持久化根路径：wal和snap
}

type raftIns struct {
	id          uint64
	kv          *Kvstore
	leader      uint64
	state       raft.StateType
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
	return r.state == raft.StateLeader && r.id == r.leader
}

func (r *raftIns) HasLeader() bool {
	return r.id != 0
}

func (r *raftIns) Leader() uint64 {
	return r.leader
}

func (r *raftIns) Closed() bool {
	if r.closed {
		return true
	}
	select {
	case _, ok := <-r.errorC:
		if !ok {
			r.closed = true
			return true
		}
	default:
	}
	return false
}

func (r *raftIns) Close() {
	close(r.confChangeC)
	close(r.kv.proposeC)
}

func (r *raftIns) updateState(c <-chan *raft.SoftState) {
	stop := false
	for !stop {
		select {
		case state := <-c:
			if state != nil {
				r.leader = state.Lead
				r.state = state.RaftState
			}
		case _, ok := <-r.errorC:
			if !ok {
				stop = true
			}
		}
	}
}

type RaftApi interface {
	Propose(key, value string)
	Lookup(key string) (string, bool)

	AddNode(nodeId uint64, url string)
	RemoveNode(nodeId uint64)

	IsLeader() bool
	HasLeader() bool
	Leader() uint64

	Closed() bool
	Close()
}

func NewRaft(c *Config) (RaftApi, error) {
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)

	peers := strings.Split(c.Cluster, ",")
	if c.Id == 0 || c.Id >= len(peers) {
		localIP := getLocalIP()
		for i := 0; i < len(peers); i++ {
			if pip, err := getIP(peers[i]); err == nil && pip == localIP {
				c.Id = i + 1
			}
		}
		if c.Id == 0 {
			return nil, errors.New("unknown raft node")
		}
	}

	// raftIns provides a commit stream for the proposals from the http api
	var kvs *Kvstore
	getSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitC, errorC, snapshotterReady, softStateC := NewRaftNode(c.Id, strings.Split(c.Cluster, ","), c.Join, c.PersistRoot, getSnapshot, proposeC, confChangeC)

	kvs = NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	if c.ServerPort != 0 {
		// 非阻塞启动kv服务端，方便测试
		go func() {
			ServeHttpKVAPI(kvs, c.ServerPort, confChangeC, errorC)
		}()
	}

	ins := &raftIns{id: uint64(c.Id), kv: kvs, confChangeC: confChangeC, errorC: errorC}
	go ins.updateState(softStateC)

	return ins, nil
}
