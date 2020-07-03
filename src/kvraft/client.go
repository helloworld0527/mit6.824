package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const RPCTimeoutInterval = 5 * 1000 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu sync.Mutex
	reqNumber int64
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
	ck.reqNumber = 0
	return ck
}

func (ck *Clerk) getUniqueReqNumber() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	result := ck.reqNumber
	ck.reqNumber++
	return  result
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
	args := GetArgs{
		key,
		ck.getUniqueReqNumber(),
	}
	reply := GetReply{}
	done := false
	leader := int(nrand()) % len(ck.servers)
	useHint := false
	for !done {
		hint := ck.sendGetRPC(&done, leader, &args, &reply)
		if hint != -1 && !useHint{ 				// take turns to use random choosing and hint
			leader = hint
			useHint = true
		} else { 								// random select another server
			leader = int(nrand()) % len(ck.servers)
			useHint = false
		}
	}
	return reply.Value
}

func (ck *Clerk) sendGetRPC(done *bool, leader int, args *GetArgs, reply *GetReply) int {
	ok := false
	t0 := time.Now()
	for time.Since(t0) < RPCTimeoutInterval && !ok {
		ok = ck.servers[leader].Call("KVServer.Get", &args, &reply)
	}
	if !ok { 									// return because of timeout
		return -1
	}
	if reply.Err == OK || reply.Err == ErrNoKey{
		*done = true
	} else if reply.Err == ErrWrongLeader {
		return reply.CurrentLeader
	} else {
		DPrintf("(error) Clerk[Get]: reply.Err argument error")
	}
	return -1
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
	args := PutAppendArgs{
		key,
		value,
		op,
		ck.getUniqueReqNumber(),
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
