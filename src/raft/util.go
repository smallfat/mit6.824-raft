package raft

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func LogLn(c int, a ...interface{}) {
	fmt.Println("goroutine id:", GetGID(), " time:", time.Now().UnixMilli(), " client:", c, " ", a)
}
