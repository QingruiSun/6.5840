package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
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

type DuplicateReply struct {
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
	duplicateMap  map[int64]DuplicateReply
	commitIndex int
	applyCond *sync.Cond
}

func (kv *KVServer) ApplyCommit() {
	for m := range kv.applyCh {
		kv.mu.Lock()
		if m.CommandValid {
			command := m.Command.(Op)
			raft.Debug("SERVER", "recieve commit, commandindex %d, type %d, key %s, value %s\n", m.CommandIndex, command.Type, command.Key, command.Value)
			if prev_reply, ok := kv.duplicateMap[command.Sequence]; !ok || prev_reply.sequence  != command.Sequence { // no duplicate request
				duplicate_reply := DuplicateReply{command.Sequence, "", ""}
				if command.Type == PUT {
					kv.keyValue[command.Key] = command.Value
					duplicate_reply.err = OK
				}
				if command.Type == APPEND {
					kv.keyValue[command.Key] = kv.keyValue[command.Key] + command.Value
					duplicate_reply.err = OK
				}
				if command.Type == GET {
					if value, ok := kv.keyValue[command.Key]; !ok {
						duplicate_reply.err = ErrNoKey
					} else {
						duplicate_reply.err = OK
						duplicate_reply.value = value
					}
				}
				kv.duplicateMap[command.ClientId] = duplicate_reply // can overwrite old sequence of the same client id
			} else { // duplicate request, so no action

			}
			kv.commitIndex = m.CommandIndex
			kv.mu.Unlock()
			kv.applyCond.Broadcast()
		} else {
			kv.mu.Unlock()
		}
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
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
	if !is_leader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	for {
		if kv.commitIndex >= start_index {
			if current_term, current_is_leader := kv.rf.GetState(); !current_is_leader || (current_term != start_term) {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			value, key_exist := kv.keyValue[args.Key]
			if key_exist {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
			return
		}
		if _, is_leader := kv.rf.GetState(); !is_leader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.applyCond.Wait()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
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

	raft.Debug("SERVER", "key %s, value %s, op %s\n", args.Key, args.Value, args.Op)
        op := Op{GET, args.Key, "", args.ClientId, args.Sequence}
        start_index, start_term, is_leader := kv.rf.Start(op)
        if !is_leader {
                reply.Err = ErrWrongLeader
                kv.mu.Unlock()
                return
        }
        for {
                if kv.commitIndex >= start_index {
			if current_term, current_is_leader := kv.rf.GetState(); !current_is_leader || (current_term != start_term) {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			if args.Op == "Put" {
				kv.keyValue[args.Key] = args.Value
			} else {
				kv.keyValue[args.Key] = kv.keyValue[args.Key] + args.Value
			}
			reply.Err = OK
			kv.mu.Unlock()
			return
                }
		if _, is_leader := kv.rf.GetState(); !is_leader {
                        reply.Err = ErrWrongLeader
                        kv.mu.Unlock()
                        return
                }
		kv.applyCond.Wait()
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
	kv.duplicateMap = make(map[int64]DuplicateReply)
	kv.check_interval = 10
	kv.commitIndex = 0
	kv.applyCond = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ApplyCommit()

	return kv
}
