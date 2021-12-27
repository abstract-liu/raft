package kvraft

import (
	"../labrpc"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	sequence int64
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
	ck.leaderId = 0
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
	seq := ck.sequence
	ck.sequence += 1

	leaderId := ck.leaderId

	args := GetArgs{Key: key, Sequence: seq}
	reply := GetReply{}
	ch := make(chan string)
	go testTimeout(ch, seq)
	for {
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Resp != OK {
			if reply.Resp == ErrNotApplied {
				//log.Printf("seq:%d send to server:%d, but not applied", seq, leaderId)
			} else {
				leaderId = (leaderId + 1) % len(ck.servers)
				//log.Printf("seq:%d call rpc err, ok:%v, err:%v, leader:%d", seq, ok, reply.Err, leaderId)
			}
		} else {
			//log.Printf("client Get key:%s, seq:%d on leader:%d", key, seq, leaderId)
			ck.leaderId = leaderId
			break
		}
	}
	ch <- "successive applied"
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
	seq := ck.sequence
	ck.sequence += 1
	leaderId := ck.leaderId

	args := PutAppendArgs{Key:key, Value: value, Op: op, Sequence: seq}
	reply := PutAppendReply{}
	ch := make(chan string)
	go testTimeout(ch, seq)
	for {
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Resp != OK {
			if reply.Resp == ErrNotApplied {
				log.Printf("seq:%d send to server:%d, but not applied", seq, leaderId)
			} else {
				leaderId = (leaderId + 1) % len(ck.servers)
				//log.Printf("seq:%d call rpc err, ok:%v, err:%v, leaderId:%d", seq, ok, reply.Resp, leaderId)
			}
		} else {
			//log.Printf("client %s key:%s seq:%d on leader:%d", op, key, seq, leaderId)
			ck.leaderId = leaderId
			break
		}
	}
	ch <- "successive applied"
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