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
const RaftTimeoutInterval = 3 * 1000 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Number 	int64			// unique number for command
	Key 	string
	Value 	string
	Kind 	string			// GET PUT APPEND
}

type replyNotify struct {
	index int
	term  int
	value string
	err   Err
}

type KVServer struct {
	mu      		sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg
	dead    		int32 							// set by Kill()

	shutdown 		chan struct{}					// notify when killed
	maxraftstate 	int 							// snapshot if log grows this big

	database 		map[string]string 				// memory kv store
	opCache			map[int64]struct{}				// record command executed
	notifies		map[int]chan replyNotify		// notify the RPC handle to return the result to client, use log index as key
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		args.ReqNumber,
		args.Key,
		"",
		GET,
	}
	err, currentLeader, value := kv.start(&op)
	reply.Err = err
	reply.CurrentLeader = currentLeader
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var kind string
	if args.Op == "Put" {
		kind = PUT
	} else {
		kind = APPEND
	}
	op := Op{
		args.ReqNumber,
		args.Key,
		args.Value,
		kind,
	}
	err, currentLeader, _ := kv.start(&op)
	reply.Err = err
	reply.CurrentLeader = currentLeader
}

//
// submit a log to raft
// commit successfully:
// the server is alive, everything goes well
// the server is alive, but not leader any more
// - replyNotify.term == applyMsg.term
// the server is down, but the log was committed by the next new leader
// - need client to send the second request for the same command
// the server is alive, but raft cannot reach agreement in time, but finally the log was committed
// - timer timeout, need second request
//
// commit fail:
// the server is alive, but the log was dropped by the new leader
// - replyNotify.term != applyMsg.term
// the server is down, the log was dropped by new leader
// - need client to send the second request for the same command
//
// return Err, currentLeader, value
//
func (kv *KVServer) start(op *Op) (Err, int, string){
	index, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		currentLeader := kv.rf.GetLeader()
		return ErrWrongLeader, currentLeader, ""
	}
	kv.mu.Lock()
	kv.notifies[index] = make(chan replyNotify, 1) 				// prevent deadlock
	kv.mu.Unlock()

	timer := time.After(RaftTimeoutInterval)
	select {
	case <- timer:
		kv.mu.Lock()
		delete(kv.notifies, index)
		kv.mu.Unlock()
		return ErrWrongLeader, kv.rf.GetLeader(), ""
	case replyResult := <- kv.notifies[index]:
		if replyResult.term == term {
			return replyResult.err, kv.rf.GetLeader(), replyResult.value
		} else {												// alive, but not leader any more
			return ErrWrongLeader, kv.rf.GetLeader(), ""
		}
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
	kv.mu.Lock()
	close(kv.shutdown)
	kv.mu.Unlock()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) waitApply() {
	for {
		select {
		case applyMsg := <- kv.applyCh:
			kv.handleApply(&applyMsg)
		case <-kv.shutdown:
			return
		}
	}
}

func (kv *KVServer) handleApply(applyMsg *raft.ApplyMsg) {
	var op Op
	var ok bool
	op, ok = applyMsg.Command.(Op)
	if !ok {
		DPrintf("(error) applyMsg.Command type error, not Op")
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	number := op.Number
	_, done := kv.opCache[number]
	notifyChan, needNotify := kv.notifies[applyMsg.CommandIndex]

	if !needNotify && done { 				// has been executed and don't need to reply to the client
		return
	}

	if !done {								// the command has not been executed
		kv.opCache[number] = struct{}{}
		switch op.Kind {
		case GET:
			value, hasKey := kv.database[op.Key]
			if hasKey {
				
			}
		}
	}

	if needNotify {							// need to reply

	}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shutdown = make(chan struct{})
	kv.database = make(map[string]string)
	kv.opCache = make(map[int64]struct{})

	go kv.waitApply()

	return kv
}
