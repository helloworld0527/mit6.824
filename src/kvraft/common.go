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
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ReqNumber int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	CurrentLeader int
}

type GetArgs struct {
	Key string
	ReqNumber int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	CurrentLeader int
	Value string
}
