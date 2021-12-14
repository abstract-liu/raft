package kvraft

import (
	"../labrpc"
	"log"
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
	//log.Printf("client Get key:%s", key)
	ck.sequence += 1
	args := GetArgs{Key: key, Sequence: ck.sequence}
	reply := GetReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			log.Printf("call rpc %v", ok)
		}
		if reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		if reply.Resp == OK {
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
	//log.Printf("client %s key:%s value:%s", op, key, value)
	ck.sequence += 1
	args := PutAppendArgs{Key:key, Value: value, Op: op, Sequence: ck.sequence}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			log.Printf("call rpc %v", ok)
		}
		if reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		if reply.Resp == OK {
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
