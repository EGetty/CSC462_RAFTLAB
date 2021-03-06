package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	//"fmt"
	"strconv"
)

const Debug = 0

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
	ClientID int64
	ReqID int
	ReqType string
	Key string
	Value string
}

type notifyArgs struct {
	Term int
	Value string
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string			//store the Key/Value
	cache map[int64]int			//cache the processed requests
	commitChan map[int]chan Op
}

func (obj Op) String() string {
	return "Op: CliendID: " + strconv.FormatInt(obj.ClientID, 10) + ", Request ID: " + strconv.Itoa(obj.ReqID) + ", Request Type: " + obj.ReqType + ", Key: " + obj.Key + ", Value: " + obj.Value
}

func (obj KVServer) String() string {
	//fmt.Print("Raft KV: ")
	//fmt.Println(obj.data)
	//fmt.Println(obj.cache)
	return "Raft KV: Me: " + strconv.Itoa(obj.me) + ", MaxRaftState: " + strconv.Itoa(obj.maxraftstate)
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{ClientID: args.ClientID, ReqID: args.ReqID, ReqType: "Get", Key: args.Key}
	ok := kv.AppendEntryToLog(entry)
	if ok {
		reply.WrongLeader = false
		kv.mu.Lock()
		val, ok := kv.data[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = val
			kv.cache[args.ClientID] = args.ReqID
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
	//println("ServerGet: " + kv.String() + " | " + args.String() + " | " + reply.String())
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{ClientID: args.ClientID, ReqID: args.ReqID, ReqType: args.Op, Key: args.Key, Value: args.Value}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
	//println("PutAppend: " + args.String() + " | " + reply.String() + " | " + kv.String())
}

func (kv *KVServer) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	ch, ok := kv.commitChan[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.commitChan[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		//println("AppendEntryToLog: " + op.String() + " | " + entry.String())
		return op == entry
	case <-time.After(1000 *time.Millisecond):
		//println("AppendEntryToLog Timeout: " + entry.String())
		return false
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
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]int)
	kv.commitChan = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		//println("Running func...")
		for {
			msg := <-kv.applyCh
			cmd, ok := msg.Command.(Op)
			kv.mu.Lock()
			v, ok := kv.cache[cmd.ClientID]
			//println("Existing ID: " + strconv.Itoa(v) + " | " + cmd.String())
			if !ok || v < cmd.ReqID {
				switch cmd.ReqType {
				case "Put":
					kv.data[cmd.Key] = cmd.Value
				case "Append":
					kv.data[cmd.Key] += cmd.Value
				}
				kv.cache[cmd.ClientID] = cmd.ReqID
				//println("PutAppend: " + kv.String())
			}
			ch, ok := kv.commitChan[msg.CommandIndex]
			if ok {
				ch <- cmd
			} else {
				kv.commitChan[msg.CommandIndex] = make(chan Op, 1)
			}
			kv.mu.Unlock()
		}
	}()
	return kv
}
