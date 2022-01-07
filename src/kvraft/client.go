package kvraft

import (
	"../labrpc"
	"log"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	server   int
	sequence int64
	mu       sync.Mutex
}

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
	ck.server = 0
	ck.sequence = nrand()
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	seq := ck.sequence
	ck.sequence += 1
	server := ck.server
	ck.mu.Unlock()

	args := GetArgs{Key: key, Sequence: seq}
	reply := GetReply{}
	//log.Printf("client start get %+v on server:%d", args, server)
	for {
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Resp != OK {
			if reply.Resp == ErrNotApplied || reply.Resp == ErrNotReady{
				//log.Printf("seq:%d send to server:%d, but not applied", seq, server)
			} else {
				server = (server + 1) % len(ck.servers)
			}
		} else {
			//log.Printf("client Get key:%s, seq:%d, value:%s on server:%d", key, seq, reply.Value,server)
			ck.mu.Lock()
			ck.server = server
			ck.mu.Unlock()
			break
		}
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	seq := ck.sequence
	ck.sequence += 1
	server := ck.server
	ck.mu.Unlock()

	args := PutAppendArgs{Key:key, Value: value, Op: op, Sequence: seq}
	reply := PutAppendReply{}
	//log.Printf("client start %s %+v on server:%d", op, args, server)
	for {
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Resp != OK {
			if reply.Resp == ErrNotApplied || reply.Resp == ErrNotReady {
				//log.Printf("seq:%d send to server:%d, but not applied", seq, server)
			} else {
				server = (server + 1) % len(ck.servers)
			}
		} else {
			//log.Printf("client %s key:%s seq:%d on server:%d", op, key, seq, server)
			ck.mu.Lock()
			ck.server = server
			ck.mu.Unlock()
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

func testTimeout(ch chan string, seq int64) {
	select{
	case <-ch:
		break

	case <-time.After(3 * time.Second):
		log.Printf("seq:%d timeout, please check", seq)
	}
}