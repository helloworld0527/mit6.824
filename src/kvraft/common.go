package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	GET 	= "GET"
	PUT 	= "PUT"
	APPEND 	= "APPEND"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	ReqNumber 	int64
	ClientId 	int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key 		string
	ReqNumber 	int64
	ClientId  	int64
}

type GetReply struct {
	Err   			Err
	Value 			string
}
