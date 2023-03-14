package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "6.5840/raft"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader           int
	mu               sync.Mutex
	clientId         int64
	sequence         int64
}

const (
	check_interval = 20 * time.Millisecond
)

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
	ck.leader = 0
	ck.clientId = nrand()
	ck.sequence = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	for {
		args := GetArgs{key, ck.clientId, ck.sequence}
		ck.sequence++
		reply := GetReply{}
		raft.Debug("CLERK", "client %d, Get server %d, key %s, sequence %d\n", ck.clientId, ck.leader, key, args.Sequence)
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.sequence--
			if !ok {
				raft.Debug("CLERK", "No leader\n")
			}
			raft.Debug("CLERK", "Continue Get server %d, key %s\n", ck.leader, key)
			time.Sleep(check_interval)
			continue
		}
		if reply.Err ==  ErrNoKey {
			raft.Debug("CLERK", "server %d, No key %s\n", ck.leader, key)
			return ""
		}
		raft.Debug("CLERK", "Succeed Get server %d, key %s, value %s\n", ck.leader, key, reply.Value)
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	for {
		args := PutAppendArgs{key, value, op, ck.clientId, ck.sequence}
		ck.sequence++
		reply := PutAppendReply{}
		raft.Debug("CLERK", "client %d, PutAppend server %d, key %s, value %s, op %s, sequence%d\n", ck.clientId, ck.leader, key, value, op, args.Sequence)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.sequence--
			if ok {
				raft.Debug("CLERK", "No leader\n")
			}
			raft.Debug("CLERK", "Continue PutAppend server %d, key %s, value %s, op %s\n", ck.leader, key, value, op)
			time.Sleep(check_interval)
			continue
		}
		raft.Debug("CLERK", "Succeed PutAppend server %d, key %s, value %s, op %s\n", ck.leader, key, value, op)
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
