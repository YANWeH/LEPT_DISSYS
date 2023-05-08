package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64 //客户端唯一标识
	seqId    int64 //单调递增的id序列号，可以避免重复的请求被反复执行
	leaderId int   //记住请求该发送给哪个服务器（leader）
	mu       sync.Mutex
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
	// You'll have to add code here.
	ck.clientId = nrand() //为客户端生成唯一id
	return ck
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
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	DPrintf("Client-%v starts Get, Key=%s\n", ck.clientId, key)
	leaderId := ck.currentLeader()

	for {
		reply := GetReply{}
		if ck.servers[leaderId].Call("KVServer.Get", &args, &reply) {
			//此id是leader
			if reply.Err == OK { //存在key对应的值，成功返回
				return reply.Value
			} else if reply.Err == ErrNoKey { //不存在
				return ""
			}
		}
		//不是最新的leader
		leaderId = ck.changeLeader()
		time.Sleep(1 * time.Millisecond)
	}
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
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	DPrintf("Client-%v starts PutAppend, Key=%s Value=%s Op=%s\n", ck.clientId, key, value, op)
	leaderId := ck.currentLeader()
	for {
		reply := PutAppendReply{}
		if ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply) {
			if reply.Err == OK { //成功发送给leader
				break
			}
		}
		//此id不是当前最新的leader
		leaderId = ck.changeLeader()
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) currentLeader() (leaderId int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	leaderId = ck.leaderId
	return
}

func (ck *Clerk) changeLeader() (leaderId int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	return ck.leaderId
}
