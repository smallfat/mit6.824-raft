package main

import (
	"6.824/labrpc"
	"6.824/raft"
)

func main() {
	var peers []*labrpc.ClientEnd
	var persister raft.Persister
	var applyCh chan raft.ApplyMsg

	peers = make([]*labrpc.ClientEnd, 1)
	persister = raft.Persister{}
	applyCh = make(chan raft.ApplyMsg)
	raft.Make(peers, 0, &persister, applyCh)
}
