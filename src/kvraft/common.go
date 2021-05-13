package kvraft

import "strconv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientID int64
	ReqID int
	Op string
	Key string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	WrongLeader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	ReqID int
}

type GetReply struct {
	Err   Err
	Value string
	WrongLeader bool
}

func (obj PutAppendArgs) String() string {
	return "PutAppendArgs: ClientID: " + strconv.FormatInt(obj.ClientID, 10) + ", Request ID: " + strconv.Itoa(obj.ReqID) + ", Key: " + obj.Key + ", Value: " + obj.Key + ", Value: " + obj.Value + ", Op: " + obj.Op
}

func (obj PutAppendReply) String() string {
	return "PutAppendReply: WrongLeader: " + strconv.FormatBool(obj.WrongLeader)
}

func (obj GetArgs) String() string {
	return "PutAppendReply: ClientID: " + strconv.FormatInt(obj.ClientID, 10) + ", Request ID: " + strconv.Itoa(obj.ReqID) + ", Key: " + obj.Key
}

func (obj GetReply) String() string {
	return "PutAppendReply: WrongLeader: " + strconv.FormatBool(obj.WrongLeader) + ". Value: " + obj.Value
}
