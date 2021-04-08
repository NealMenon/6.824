package raft

import (
	"fmt"
	"log"
)

// Debugging
const enabled = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if enabled {
		log.Printf(format, a...)
	}
	return
}

func TestDebug(format string, a ...interface{}) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if enabled {
		format = fmt.Sprintf("[TEST]\t") + format
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) Debug(format string, a ...interface{}) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if enabled {
		format = fmt.Sprintf("[%v]\t", rf.me) + format
		log.Printf(format, a...)
	}
	return
}
