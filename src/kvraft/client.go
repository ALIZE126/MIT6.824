package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Leader_server int
	mu            sync.Mutex
	check_Leader  chan int
	Client_id     int64
	Sequence_Num  int
}

//

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	//ck.check_Leader = sync.NewCond(&ck.mu)
	ck.check_Leader = make(chan int)
	ck.Leader_server = 0
	ck.Sequence_Num = 0
	ck.Client_id = nrand()
	//go ck.Ping_Leader()
	return ck
}

//func (ck *Clerk) Ping_Leader() {
//	for {
//		for server := 0; server < len(ck.servers); server++ {
//			//go func(server int) {
//			//	ck.mu.Lock()
//			//	defer ck.mu.Unlock()
//			args := StateArgs{}
//			reply := StateReply{}
//			ok := ck.servers[server].Call("KVServer.Getstate", &args, &reply)
//			if ok {
//				if reply.IsLeader && reply.Server_index != -1 {
//					//if reply.Server_index == ck.Leader_server {
//					//	return
//					//}
//					//ck.Leader_server = reply.Server_index
//					//ck.mu.Unlock()
//					ck.check_Leader <- reply.Server_index
//					//log.Println("Client:Leader server is", ck.Leader_server)
//				}
//			}
//			//}(server)
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//
//}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	//ck.mu.Lock()
	//defer ck.mu.Unlock()
	args := &GetArgs{}
	args.Key = key
	args.Client_id = ck.Client_id
	reply := &GetReply{}
	ck.Sequence_Num += 1
	Server_id := ck.Leader_server
	//Command_id := ck.Sequence_Num + 1
	args.Sequence_Num = ck.Sequence_Num
	//ck.mu.Unlock()
	// You will have to modify this function.
	//for server := 0; server < len(ck.servers); server++ {
	for ; ; Server_id = (Server_id + 1) % len(ck.servers) {
		//log.Println(ck.Client_id, "send GET to server:", ck.Leader_server, "Sequence_num:", args.Sequence_Num)
		ok := ck.servers[Server_id].Call("KVServer.Get", args, reply)
		//ck.mu.Lock()
		if reply.Err == ErrWrongLeader || !ok || reply.Err == ErrTimeout {
			//ck.mu.Unlock()
			continue
		}
		if reply.Err == OK {
			ck.Leader_server = Server_id
			//ck.Sequence_Num = Command_id
			//ck.Sequence_Num = Command_id
			//ck.mu.Unlock()
			return reply.Value
		} else if reply.Err == ErrNoKey {
			//ck.mu.Unlock()
			return ""
		}
	}
	//}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//ck.mu.Lock()
	//ck.check_Leader.Wait()
	//if ck.Leader_server == -1 {
	//	ck.mu.Unlock()
	//}

	//defer ck.mu.Unlock()
	//ck.Leader_server = <-ck.check_Leader
	//ck.mu.Lock()
	ck.Sequence_Num += 1
	//Command_id := ck.Sequence_Num + 1
	args := &PutAppendArgs{}
	args.Op = op
	args.Key = key
	args.Value = value
	args.Client_id = ck.Client_id
	args.Sequence_Num = ck.Sequence_Num
	reply := &PutAppendReply{}
	Server_id := ck.Leader_server
	//Leader_server := ck.Leader_server
	//ck.mu.Unlock()
	for ; ; Server_id = (Server_id + 1) % len(ck.servers) {
		//log.Println(ck.Client_id, "send PutAppend to server:", ck.Leader_server, "Sequence_num:", args.Sequence_Num)
		ok := ck.servers[Server_id].Call("KVServer.PutAppend", args, reply)
		//ck.mu.Lock()
		if reply.Err == ErrWrongLeader || !ok || reply.Err == ErrTimeout {
			//ck.mu.Unlock()
			continue
		}
		if reply.Err == OK {
			ck.Leader_server = Server_id
			//ck.Sequence_Num = Command_id
			//ck.mu.Unlock()
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
