package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET = 0
	APPEND = 1
	PUT = 2
)

// We introduce ExecutionTimeOut for this case, when leader start a log, then the leader lose
// it's authority, so it can't commit it's log, it will block, and then block the client too(may be the bug of lab).
// So we add a timer to let client resend command.
const ExecutionTimeout = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type   int
	Key    string
	Value  string
	ClientId int64
	Sequence int64
}

type ApplyResult struct {
	sequence int64
	value string
	err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	check_interval  int
	keyValue   map[string]string
	duplicateMap  map[int64]ApplyResult
	commitIndex int
	chans map[int]chan ApplyResult
}

func (kv *KVServer) ApplyCommit() {
	for m := range kv.applyCh {
		raft.Debug("SERVER", "server %d want get lock apply a\n", kv.me)
                command := m.Command.(Op)
                result := ApplyResult{command.Sequence, "", ""}
		raft.Debug("SERVER", "server %d recieve commit, commandindex %d, type %d, key %s, value %s\n", kv.me, m.CommandIndex, command.Type, command.Key, command.Value)
		kv.mu.Lock()
		raft.Debug("SERVER", "server %d succeed get lock apply b\n", kv.me)
		if m.CommandValid {
			if prev_reply, ok := kv.duplicateMap[command.ClientId]; !ok || prev_reply.sequence  != command.Sequence { // no duplicate request
				if command.Type == PUT {
					kv.keyValue[command.Key] = command.Value
					result.err = OK
				}
				if command.Type == APPEND {
					kv.keyValue[command.Key] = kv.keyValue[command.Key] + command.Value
					raft.Debug("SERVER", "server %d update key %s value to %s\n", kv.me, command.Key, kv.keyValue[command.Key])
					result.err = OK
				}
				if command.Type == GET {
					raft.Debug("SERVER", "server %d recieve GET commit\n", kv.me)
					if value, ok := kv.keyValue[command.Key]; !ok {
						result.err = ErrNoKey
						raft.Debug("SERVER", "server %d no key\n", kv.me)
					} else {
						result.err = OK
						result.value = value
						raft.Debug("SERVER", "server %d get key value %s\n", kv.me, value)
					}
				}
				kv.duplicateMap[command.ClientId] = result // can overwrite old sequence of the same client id
			} else { // duplicate request, so no action
				result.err = kv.duplicateMap[command.ClientId].err
				result.value = kv.duplicateMap[command.ClientId].value
			}
			kv.commitIndex = m.CommandIndex
			if _, ok := kv.chans[m.CommandIndex]; ok {
				raft.Debug("SERVER", "server %d recieve commit, commandindex %d, type %d, key %s, value %s, write to chans\n", kv.me, m.CommandIndex, command.Type, command.Key, command.Value)
				ch := kv.chans[m.CommandIndex]
				kv.mu.Unlock()
				ch <- result
			} else {
				kv.mu.Unlock()
			}
			raft.Debug("SERVER", "server %d recieve commit, commandindex %d, type %d, key %s, value %s, finished\n", kv.me, m.CommandIndex, command.Type, command.Key, command.Value)
		} else {
			kv.mu.Unlock()
		}
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	raft.Debug("SERVER", "server %d want get lock get a\n", kv.me)
	kv.mu.Lock()
	raft.Debug("SERVER", "server %d succeed get lock get a\n", kv.me)
	if _, is_leader := kv.rf.GetState(); !is_leader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if prev_reply, ok := kv.duplicateMap[args.ClientId]; ok && prev_reply.sequence == args.Sequence {
		reply.Err = prev_reply.err
		reply.Value = prev_reply.value
		kv.mu.Unlock()
		return
	}

	op := Op{GET, args.Key, "", args.ClientId, args.Sequence}
	start_index, start_term, is_leader := kv.rf.Start(op)
	raft.Debug("SERVER", "server %d get %s in start index %d\n", kv.me, args.Key, start_index)
	kv.chans[start_index] = make(chan ApplyResult, 1)
	ch := kv.chans[start_index]
	if !is_leader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	for {
		kv.mu.Unlock()
		select {
		case result := <-ch:
			reply.Value = result.value
			reply.Err = result.err
			raft.Debug("SERVER", "server %d get chans recieve data in  %d\n", kv.me, start_index)
		case <-time.After(ExecutionTimeout):
			raft.Debug("SERVER", "server %d get chans recieve data time out in %d\n", kv.me, start_index)
			reply.Err = ErrTimeout
		}
		raft.Debug("SERVER", "server %d want get lock get b\n", kv.me)
		kv.mu.Lock()
		raft.Debug("SERVER", "server %d succeed get lock get b\n", kv.me)
		if current_term, current_is_leader := kv.rf.GetState(); !current_is_leader || (current_term != start_term) {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	raft.Debug("SERVER", "%d\n", kv.me)
	raft.Debug("SERVER", "server %d want get lock put a\n", kv.me)
	kv.mu.Lock()
	raft.Debug("SERVER", "server %d succeed get lock put a\n", kv.me)
        if _, is_leader := kv.rf.GetState(); !is_leader {
                reply.Err = ErrWrongLeader
                kv.mu.Unlock()
                return
        }

        if prev_reply, ok := kv.duplicateMap[args.ClientId]; ok && prev_reply.sequence == args.Sequence {
                reply.Err = prev_reply.err
                kv.mu.Unlock()
                return
        }

	raft.Debug("SERVER", "server %d, key %s, value %s, op %s\n", kv.me, args.Key, args.Value, args.Op)
	op_type := PUT
	if args.Op == "Append" {
		op_type = APPEND
	}
        op := Op{op_type, args.Key, args.Value, args.ClientId, args.Sequence}
        start_index, start_term, is_leader := kv.rf.Start(op)
	kv.chans[start_index] = make(chan ApplyResult, 1)
	ch := kv.chans[start_index]
        if !is_leader {
                reply.Err = ErrWrongLeader
                kv.mu.Unlock()
                return
        }
        for {
		kv.mu.Unlock()
		raft.Debug("SERVER", "server %d block on putappend chans before recieve data in %d\n", kv.me, start_index)
		select {
		case result := <-ch:
			reply.Err = result.err
			raft.Debug("SERVER", "server %d putappend chans recieve data in %d\n", kv.me, start_index)
		case <-time.After(ExecutionTimeout):
			reply.Err = ErrTimeout
		}
		raft.Debug("SERVER", "server %d want get lock put b\n", kv.me)
		kv.mu.Lock()
		raft.Debug("SERVER", "server %d succeed get lock put b\n", kv.me)
		if current_term, current_is_leader := kv.rf.GetState(); !current_is_leader || (current_term != start_term) {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		return
        }
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.keyValue = make(map[string]string)
	kv.check_interval = 10
	kv.commitIndex = 0
	kv.chans = make(map[int]chan ApplyResult)
	kv.duplicateMap = make(map[int64]ApplyResult)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ApplyCommit()

	return kv
}
