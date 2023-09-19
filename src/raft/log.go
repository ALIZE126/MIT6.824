package raft

import "log"

type Log_info struct {
	Term    int
	Command interface{}
}
type Log struct {
	log_index []Log_info
	index0    int
}

func (l *Log) append(log_info Log_info) {
	l.log_index = append(l.log_index, log_info)
}
func (l *Log) start() int {
	return l.index0
}
func (l *Log) slice(index int) []Log_info {
	return l.log_index[index-l.index0:]
}
func (l *Log) last_index() int {
	return l.index0 + len(l.log_index) - 1
}
func (l *Log) entry(index int) *Log_info {
	return &(l.log_index[index-l.index0])
}
func (l *Log) lastentry() *Log_info {
	return l.entry(l.last_index())
}
func (log *Log) Get_Loginfo(index int) Log_info {
	return log.log_index[index]
}
func (l *Log) mkLogEmpty() Log {
	return Log{make([]Log_info, 1), 0}
}
func (l *Log) Cut(index int) {
	l.log_index = l.log_index[index:]
	log.Println("after cut Log:", l.log_index)
	l.index0 = index
}

//func(log *Log)Log_Append(index int)[]map[int]interface{}{
//
//}
