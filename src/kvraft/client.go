package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

const RPCTimeoutInterval = 1 * 1000 * time.Millisecond

//
// a single clerk only send one RPC to server at a time, so it won't need concurrent lock
//
type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	reqNumber 	int64					// global request sequence number
	leader		int						// record current leader
	id 			int64					// identify client
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
	ck.leader = 0
	ck.id = nrand()
	return ck
}

func (ck *Clerk) getUniqueReqNumber() int64 {
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
		ck.id,
	}
	done := false
	leader := ck.leader
	for !done {
		ok := false
		t0 := time.Now()
		reply := GetReply{}
		for time.Since(t0) < RPCTimeoutInterval && !ok {
			ok = ck.servers[leader].Call("KVServer.Get", &args, &reply)
		}

		if ok { 								// if ok=false, timeout
			if reply.Err == OK || reply.Err == ErrNoKey{
				done = true
				ck.leader = leader
				return reply.Value
			}
		}
		leader = (leader+1) % len(ck.servers)
	}
	return ""
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
		ck.id,
	}
	done := false
	leader := ck.leader
	for !done {
		ok := false
		t0 := time.Now()
		reply := PutAppendReply{}
		for time.Since(t0) < RPCTimeoutInterval && !ok {
			ok = ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		}

		if ok { 								// if ok=false, timeout
			if reply.Err == OK || reply.Err == ErrNoKey{
				done = true
				ck.leader = leader
				return
			}
		}
		leader = (leader+1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
