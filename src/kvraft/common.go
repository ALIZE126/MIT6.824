package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_id    int64
	Sequence_Num int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client_id    int64
	Sequence_Num int
}

type GetReply struct {
	Err   Err
	Value string
}
type StateArgs struct {
	// You'll have to add definitions here.
}

type StateReply struct {
	IsLeader     bool
	Server_index int
}
