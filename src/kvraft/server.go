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
//todo: 存在如果断开前最后一个无法apply 的问题，我们需要想一下这个该如何解决


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
	applySeqs map[int64]bool
	kvStore  map[string]string
	applyChs map[int64]chan string
}


//这里似乎有问题没办法执行成功或超时操作
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Resp = ErrWrongLeader
		//log.Printf("server %d seq: %d wrong leader", kv.me, args.Sequence)
		return
	}
	//log.Printf("server Get key:%s seq:%d on server:%d",  args.Key, args.Sequence,  kv.me)

	//这里有问题 有大问题 这里有时序问题！！！！！
	kv.mu.Lock()
	if value, exist := kv.applySeqs[args.Sequence]; exist && value{
		reply.Resp = OK
		//i forget about this fucking thing
		reply.Value = kv.kvStore[args.Key]
		kv.mu.Unlock()
		return
	} else if exist && !value {
		//log.Printf("server %d seq: %d exist but not applied", kv.me, args.Sequence)
		reply.Resp = ErrNotApplied
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Key: args.Key, Operation: "Get", Sequence: args.Sequence}
	kv.rf.Start(op)
	//log.Printf("index:%d term:%d isLeader:%v", index, term, isLeader)

	ch := make(chan string, 1)
	kv.mu.Lock()
	kv.applyChs[args.Sequence] = ch
	kv.applySeqs[args.Sequence] = false
	kv.mu.Unlock()

	select {
	case <-ch:
		reply.Resp = OK
		kv.mu.Lock()
		reply.Value = kv.kvStore[args.Key]
		kv.mu.Unlock()
	case <-time.After(3 * time.Second):
		log.Printf("Get wait time passed")
		reply.Resp = ErrTimeout
	}

}
//有一种情况，就是server 接受到了rpc， 加入了其state， 但是这时候断网了
//然后server 又重新选举变成了leader， 但由于没有新的在他这个任期内的消息
//导致这一条信息没办法提交成功
//对应的情形就是 ErrNotApplied 因为client 是顺序的执行
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Resp = ErrWrongLeader
		//log.Printf("server %d seq: %d wrong leader", kv.me, args.Sequence)
		return
	}
	//log.Printf("server %s key:%s value:%s seq:%d on server:%d", args.Op, args.Key, args.Value, args.Sequence, kv.me)

	kv.mu.Lock()
	if value, exist := kv.applySeqs[args.Sequence]; exist && value{
		reply.Resp = OK
		kv.mu.Unlock()
		return
	} else if exist && !value {
		//log.Printf("server %d seq: %d exist but not applied", kv.me, args.Sequence)
		reply.Resp = ErrNotApplied
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Key: args.Key, Value: args.Value, Operation: args.Op, Sequence: args.Sequence}
	kv.rf.Start(op)
	log.Printf("server %s key:%s value:%s seq:%d on server:%d", args.Op, args.Key, args.Value, args.Sequence, kv.me)

	ch := make(chan string, 1)
	kv.mu.Lock()
	kv.applyChs[args.Sequence] = ch
	kv.applySeqs[args.Sequence] = false
	kv.mu.Unlock()

	select {
	case <-ch:
		reply.Resp = OK
	case <-time.After(3 * time.Second):
		log.Printf("PutAppend wait time passed %d", args.Sequence)
		reply.Resp = ErrTimeout
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
	kv.applySeqs = make(map[int64]bool)
	kv.kvStore = make(map[string]string)
	kv.applyChs = make(map[int64]chan string)
	go kv.applyRaftLog()

	return kv
}

func (kv *KVServer) applyRaftLog(){
	for {
		raftLog := <-kv.applyCh
		kv.mu.Lock()
		commandValid := raftLog.CommandValid
		if !commandValid {
			op := Op{Operation: "Start"}
			kv.rf.Start(op)
			kv.mu.Unlock()
			continue
		}

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

		if _, isLeader := kv.rf.GetState(); isLeader{
			log.Printf("server %d apply log%+v ", kv.me, raftOp)
		}
		kv.applySeqs[raftOp.Sequence] = true
		ch, exist := kv.applyChs[raftOp.Sequence]
		// 这里存在如果超时卡住的问题
		if exist {
			select {
				case ch <- OK:
					//log.Printf("fuck")
				case <- time.After(3 * time.Second):
					//log.Printf("fuck")
			}
		}
		kv.mu.Unlock()
	}
}