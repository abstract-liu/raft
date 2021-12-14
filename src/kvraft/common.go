package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
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
	Sequence int64
}

type PutAppendReply struct {
	Resp string
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Sequence int64
}

type GetReply struct {
	Resp string
	Err   Err
	Value string
}
