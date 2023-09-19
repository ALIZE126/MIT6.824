package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		log.Printf(format, a...)
	}
	return
}

const (
	Leader    = 1
	Candidate = 2
	Follower  = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd        Command_construct
	Option     string
	Client_id  int64
	Command_id int
	//Client_index int64
}
type Command_construct struct {
	Key   string
	Value string
	//Option string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Database    []map[string]string
	IsLeader    bool
	Get_Value   chan string
	state       state
	End_Raft    chan Err
	check       *sync.Cond
	check_point bool
	//Client_info map[int64][]Apply_Context
	Client_info map[int64]Apply_Context
	LastApplied int
	Raft_Chan   map[int]chan Apply_Context
}
type state int

//	type Context struct {
//		Reply_Err   Err
//		Reply_Value string
//	}
type Apply_Context struct {
	Command_id  int
	Reply_Err   Err
	Reply_Value string
	Term        int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//log.Println(args.Client_id, "early and server come here is Leader:")
	kv.mu.Lock()
	if context, ok := kv.Client_info[args.Client_id]; ok {
		//log.Println("args.Client_id:", args.Client_id, "GET context.Command_id:", context.Command_id)
		if context.Command_id >= args.Sequence_Num {
			reply.Err = context.Reply_Err
			reply.Value = context.Reply_Value
			kv.mu.Unlock()
			return
		}
		//kv.mu.Unlock()
		//reply.Err = kv.Client_info[args.Client_id][args.Sequence_Num-1].Reply_Err
		//reply.Value = kv.Client_info[args.Client_id][args.Sequence_Num-1].Reply_Value
	}
	kv.mu.Unlock()
	op_raft := Op{}
	op_raft.Cmd.Key = args.Key
	op_raft.Option = "Get"
	op_raft.Client_id = args.Client_id
	op_raft.Command_id = args.Sequence_Num
	//op_raft.Client_index = args.Client_id
	//log.Println("op_raft.Command_id:", op_raft.Command_id)

	index, _, IsLeader := kv.rf.Start(op_raft)
	if !IsLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	//log.Println(args.Client_id, "after and server come here is Leader:")
	apply_ch := make(chan Apply_Context)
	kv.mu.Lock()
	kv.Raft_Chan[index] = apply_ch
	kv.mu.Unlock()
	select {
	//case reply.Err = <-kv.End_Raft:
	case context := <-apply_ch:
		reply.Value = context.Reply_Value
		reply.Err = context.Reply_Err
		//reply.Value = <-kv.Get_Value
		//kv.mu.Lock()
		//if context.Term > Term {
		//	reply.Err = ErrWrongLeader
		//	kv.mu.Unlock()
		//	return
		//}
		//reply.Err = context
		//reply.Value = context.Reply_Value
		//kv.Client_info[args.Client_id] = context
		//kv.LastApplied=len(kv.Client_info[args.Client_id])
		//kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.CloseChan(index)
	//close(kv.End_Raft)
	return
}
func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if isChanClose(kv.Raft_Chan[index]) {
		close(kv.Raft_Chan[index])
	} else {
		return
	}

}

//	func (kv *KVServer) Getstate(args *StateArgs, reply *StateReply) {
//		// Your code here.
//		_, Isleader := kv.rf.GetState()
//		kv.mu.Lock()
//		reply.IsLeader = Isleader
//		reply.Server_index = -1
//		kv.IsLeader = Isleader
//		if reply.IsLeader {
//			reply.Server_index = kv.me
//			log.Println(kv.me, " is Leader")
//		}
//		kv.mu.Unlock()
//		return
//	}
func (kv *KVServer) ApplyStore(ApplyMsg raft.ApplyMsg) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	OP := ApplyMsg.Command.(Op)
	Command := OP.Cmd
	Option := OP.Option
	Index := OP.Command_id
	//Index := ApplyMsg.CommandIndex
	Key := Command.Key
	Value := Command.Value
	Client_id := OP.Client_id
	//Log_Index := ApplyMsg.CommandIndex
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	if commandContext, ok := kv.Client_info[Client_id]; ok && commandContext.Command_id >= Index {
		//DPrintf("kvserver[%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", kv.me, ApplyMsg, commandContext)
		//commonReply = commandContext.Reply_Err
		return
	}
	if Option == "Put" {
		for index := 0; index < len(kv.Database); index++ {
			if kv.Database[index][Key] != "" {
				kv.Database[index][Key] = Value
				context := Apply_Context{
					Reply_Err:   OK,
					Reply_Value: "",
					Command_id:  Index,
				}
				//log.Println("DataBase:", kv.Database)
				kv.mu.Lock()
				kv.Client_info[Client_id] = context
				kv.LastApplied = Index
				//kv.Client_info[Client_id] = append(kv.Client_info[Client_id], context)
				//kv.mu.Unlock()
				//kv.End_Raft <- OK
				if relpych, ok := kv.Raft_Chan[Index]; ok {
					kv.mu.Unlock()
					relpych <- context
				} else {
					kv.mu.Unlock()
				}
				return
			}
		}
		new_map := map[string]string{
			Key: Value,
		}
		kv.Database = append(kv.Database, new_map)
		context := Apply_Context{
			Reply_Err:   OK,
			Reply_Value: "",
			Command_id:  Index,
		}
		kv.mu.Lock()
		kv.Client_info[Client_id] = context
		//kv.Client_info[Client_id] = append(kv.Client_info[Client_id], context)
		//log.Println("DataBase:", kv.Database)
		//kv.mu.Unlock()
		//kv.End_Raft <- OK
		if relpych, ok := kv.Raft_Chan[Index]; ok {
			kv.mu.Unlock()
			relpych <- context
		} else {
			kv.mu.Unlock()
		}
	} else if Option == "Append" {
		for index := 0; index < len(kv.Database); index++ {
			if kv.Database[index][Key] != "" {
				New_Value := kv.Database[index][Key] + Value
				kv.Database[index][Key] = New_Value
				context := Apply_Context{
					Reply_Err:   OK,
					Reply_Value: "",
					Command_id:  Index,
				}
				kv.mu.Lock()
				kv.Client_info[Client_id] = context
				//kv.Client_info[Client_id] = append(kv.Client_info[Client_id], context)
				//log.Println("DataBase:", kv.Database)
				//kv.mu.Unlock()
				//kv.End_Raft <- OK
				if relpych, ok := kv.Raft_Chan[Index]; ok {
					kv.mu.Unlock()
					relpych <- context
				} else {
					kv.mu.Unlock()
				}
				return
			}
		}
		new_map := map[string]string{
			Key: Value,
		}
		kv.Database = append(kv.Database, new_map)
		//log.Println("DataBase:", kv.Database)
		context := Apply_Context{
			Reply_Err:   OK,
			Reply_Value: "",
			Command_id:  Index,
		}
		kv.mu.Lock()
		kv.Client_info[Client_id] = context
		//kv.Client_info[Client_id] = append(kv.Client_info[Client_id], context)
		//kv.mu.Unlock()
		//kv.End_Raft <- OK
		if relpych, ok := kv.Raft_Chan[Index]; ok {
			kv.mu.Unlock()
			relpych <- context
		} else {
			kv.mu.Unlock()
		}
	} else {
		for index := 0; index < len(kv.Database); index++ {
			if kv.Database[index][Key] != "" {
				Value_Get := kv.Database[index][Key]
				context := Apply_Context{
					Reply_Err:   OK,
					Reply_Value: Value_Get,
					Command_id:  Index,
					//Term:
				}
				kv.mu.Lock()
				kv.Client_info[Client_id] = context
				//kv.Client_info[Client_id] = append(kv.Client_info[Client_id], context)
				//kv.mu.Unlock()
				//kv.End_Raft <- OK
				if relpych, ok := kv.Raft_Chan[Index]; ok {
					kv.mu.Unlock()
					relpych <- context
				} else {
					kv.mu.Unlock()
				}
				//kv.Raft_Chan[Index] <- context
				//kv.Get_Value <- Value_Get
				return
			}
		}
		context := Apply_Context{
			Reply_Err:   ErrNoKey,
			Reply_Value: "",
			Command_id:  Index,
		}
		kv.mu.Lock()
		kv.Client_info[Client_id] = context
		//kv.Client_info[Client_id] = append(kv.Client_info[Client_id], context)
		//kv.mu.Unlock()
		if relpych, ok := kv.Raft_Chan[Index]; ok {
			kv.mu.Unlock()
			relpych <- context
		} else {
			kv.mu.Unlock()
		}
		//kv.End_Raft <- ErrNoKey
		//kv.Get_Value <- ""
	}

}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	//if kv.IsLeader {
	//log.Println(args.Client_id, "early and server come here is Leader:")
	kv.mu.Lock()
	if context, ok := kv.Client_info[args.Client_id]; ok {
		//log.Println("args.Client_id:", args.Client_id, "PutAppend context.Command_id:", context.Command_id)
		if context.Command_id == args.Sequence_Num {
			//reply.Err = context[args.Sequence_Num-1].Reply_Err
			reply.Err = context.Reply_Err
			//reply.Value = context.Reply_Value
			//log.Println("come here to catch the repeat order")
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//log.Println(args.Client_id, "AFTER and server come here is Leader:")
	op_raft := Op{}
	op_raft.Cmd.Key = args.Key
	op_raft.Cmd.Value = args.Value
	op_raft.Option = args.Op
	op_raft.Client_id = args.Client_id
	op_raft.Command_id = args.Sequence_Num
	//op_raft.Client_index = args.Client_id
	//log.Println(kv.me, "is Leader:", kv.IsLeader)
	//kv.mu.Unlock()
	index, _, IsLeader := kv.rf.Start(op_raft)
	if !IsLeader {
		reply.Err = ErrWrongLeader
		return
	}
	apply_ch := make(chan Apply_Context)
	kv.mu.Lock()
	kv.Raft_Chan[index] = apply_ch
	kv.mu.Unlock()
	select {
	//case context := <-kv.End_Raft:
	case context := <-apply_ch:
		//kv.mu.Lock()
		//if context.Term > Term {
		//	reply.Err = ErrWrongLeader
		//	kv.mu.Unlock()
		//	return
		//}
		reply.Err = context.Reply_Err

		//kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.CloseChan(index)
	//close(kv.End_Raft)
	//kv.Start_Raft <- op_raft
	//<-kv.End_Raft
	//kv.mu.Lock()
	//kv.check.Wait()
	//reply.Err = "no such key"
	return
}
func isChanClose(ch chan Apply_Context) bool {
	select {
	case _, received := <-ch:
		return !received
	default:
	}
	return false
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) Service() {
	for !kv.killed() {
		//time.Sleep(1000 * time.Millisecond)
		//kv.mu.Lock()
		//if kv.rf.FullGetState() == Candidate {
		//	time.Sleep(1000 * time.Millisecond)
		//	continue
		//} else {
		//	if kv.rf.FullGetState() == Leader {
		//		log.Println("this is leader")
		//		kv.IsLeader = true
		//		kv.state = Leader
		//	} else {
		//		kv.IsLeader = false
		//		kv.state = Follower
		//		log.Println("this is follower")
		//	}
		//}
		//kv.mu.Lock()
		//if kv.check_point {
		//	if kv.IsLeader {
		//		kv.mu.Unlock()
		//		//op_raft := Op{}
		//		log.Println("Leader service")
		//		op_raft := <-kv.Start_Raft
		//		log.Println("to_Raft")
		//		kv.rf.Start(op_raft.Cmd)
		//		logs := <-kv.applyCh
		//		log.Println("here is the log:", logs)
		//		kv.End_Raft <- true
		//		//kv.check.Broadcast()
		//	} else {
		//		kv.mu.Unlock()
		//		log.Println("Follower service")
		//		<-kv.applyCh
		//		kv.End_Raft <- true
		//		//kv.check.Broadcast()
		//	}
		//} else {
		//	time.Sleep(50 * time.Millisecond)
		//}
		//kv.mu.Unlock()
		//time.Sleep(50 * time.Millisecond)
		//log.Println(kv.me, "come here")
		select {
		case Log_commited := <-kv.applyCh:
			//log.Println(kv.me, "service here")
			if Log_commited.CommandValid {
				kv.ApplyStore(Log_commited)
			} else {
				log.Println(kv.me, "bug here")
			}
		}

	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.Get_Value = make(chan string)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.check = sync.NewCond(&kv.mu)
	kv.End_Raft = make(chan Err)
	kv.check_point = false
	//kv.Client_info = make(map[int64][]Apply_Context)
	kv.Client_info = make(map[int64]Apply_Context)
	kv.Raft_Chan = make(map[int]chan Apply_Context)
	kv.LastApplied = 0
	// You may need initialization code here.
	go kv.Service()
	return kv
}
