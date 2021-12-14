package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Operation string
	Sequence int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastSeq int64
	kvStore map[string]string
	applyInfo map[int64]chan string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Key: args.Key, Operation: "Get", Sequence: args.Sequence}
	kv.rf.Start(op)
	//log.Printf("index:%d term:%d isLeader:%v", index, term, isLeader)

	ch := make(chan string)
	kv.applyInfo[args.Sequence] = ch

	msg := <-ch
	if msg == OK {
		reply.Resp = OK
		kv.mu.Lock()
		reply.Value = kv.kvStore[args.Key]
		kv.mu.Unlock()
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Key: args.Key, Value: args.Value, Operation: args.Op, Sequence: args.Sequence}
	kv.rf.Start(op)
	//log.Printf("index:%d term:%d isLeader:%v", index, term, isLeader)

	ch := make(chan string)
	kv.applyInfo[args.Sequence] = ch

	select {
	case <-ch:
		//log.Printf("receive fuck%s", msg)
		reply.Resp = OK
	case <-time.After(1 * time.Second):
		log.Printf("wait time passed")
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastSeq = 0
	kv.kvStore = make(map[string]string)
	kv.applyInfo = make(map[int64]chan string)
	go kv.applyRaftLog()

	return kv
}

func (kv *KVServer) applyRaftLog(){
	for {
		raftLog := <-kv.applyCh
		kv.mu.Lock()
		raftOp := raftLog.Command.(Op)
		switch raftOp.Operation {
		case "Put":
			kv.kvStore[raftOp.Key] = raftOp.Value
			break
		case "Append":
			kv.kvStore[raftOp.Key] += raftOp.Value
			//log.Printf("server append %s", kv.kvStore[raftOp.Key])
			break
		}

		//log.Printf("server %d apply log%+v ", kv.me, raftOp )
		kv.lastSeq = raftOp.Sequence
		ch, exist := kv.applyInfo[raftOp.Sequence]
		if exist {
			ch <- OK
		}
		kv.mu.Unlock()
	}
}