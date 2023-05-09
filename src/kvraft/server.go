package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	LogIndex int    //写入raft log时的index
	LogTerm  int    //写入raft log时的term
	Type     string //put append get
	Key      string
	Value    string
	ClientId int64
	SeqId    int64
}

//op请求上下文,等待raft提交期间的op上下文，用于唤醒阻塞的RPC
//客户端给raft一个请求，不能一直忙等待replyRPC
type OpContext struct {
	op          *Op
	committed   chan byte
	wrongLeader bool //index位置的term不一致，说明leader换过了
	ignored     bool //req id过期，导致该日志被跳过

	//Get操作结果
	keyExist bool
	value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	kvStore map[string]string  //kv存储
	reqMap  map[int]*OpContext //存储正在进行中的RPC调用，log index -> 请求上下文
	seqMap  map[int64]int64    //记录每个客户端已提交的最大请求Id，客户端id -> 客户端seq
	//lastAppliedIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK

	//记录此请求的操作信息
	op := &Op{
		Type:     OpGet,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	//写入raft层
	var isLeader bool
	op.LogIndex, op.LogTerm, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		//保存此请求的上下文，等待raft发送commit信息
		//此过程中，raft中的leader可能会发生变更从而覆盖了此请求提交的index位置
		//同时新leader不会再发送commitRPC给此请求
		//不过，此请求的RPC会超时而重发
		kv.reqMap[op.LogIndex] = opCtx
	}()

	//RPC结束后清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.LogIndex]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.LogIndex)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	//select循环检测两个通道，要么提交成功，要么失败
	select {
	case <-opCtx.committed: //如果响应成功
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else if !opCtx.keyExist { //key不存在
			reply.Err = ErrNoKey
		} else { //key存在且返回成功，记录返回值
			reply.Value = opCtx.value
		}
	case <-timer.C: //如果2秒都没有收到leader发送的响应，让客户端重试
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK

	//记录此请求的操作信息
	op := &Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	//写入raft层
	var isLeader bool
	op.LogIndex, op.LogTerm, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	opCtx := newOpContext(op)
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		//保存此请求的上下文，等待raft发送commit信息
		//此过程中，raft中的leader可能会发生变更从而覆盖了此请求提交的index位置
		//同时新leader不会再发送commitRPC给此请求
		//不过，此请求的RPC会超时而重发
		kv.reqMap[op.LogIndex] = opCtx
	}()

	//请求RPC结束后清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.LogIndex]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.LogIndex)
			}
		}
	}()

	timer := time.NewTimer(2000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-opCtx.committed: //如果响应成功
		if opCtx.wrongLeader { //同样的logindex位置logterm不一样，说明leader变了，需要客户端向新leader重新写入
			reply.Err = ErrWrongLeader
		} else if opCtx.ignored {
			//说明req id过期了，该请求被忽略，直接告诉客户端OK即可
			reply.Err = OK
		}
	case <-timer.C: //2秒都没收到leader的响应，让客户端重试
		reply.Err = ErrWrongLeader
	}
}

func newOpContext(op *Op) (opCtx *OpContext) {
	opCtx = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
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
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.kvStore = make(map[string]string)
	kv.reqMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)

	go kv.applyMonitor()

	return kv
}

//循环等待客户端发来请求。然后进行相应处理
func (kv *KVServer) applyMonitor() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh: //raft层完成了请求日志的复制，返回给服服务器端
			cmd := msg.Command
			index := msg.CommandIndex

			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()

				//cmd就是请求操作信息，将接口类型转换为Op结构类型
				op := cmd.(*Op)

				opCtx, existOp := kv.reqMap[index]
				prevSeq, existSeq := kv.seqMap[op.ClientId]
				kv.seqMap[op.ClientId] = op.SeqId

				if existOp { //如果存在等待结果的RPC，判断状态是否与发送时的一致
					if opCtx.op.LogTerm != op.LogTerm {
						opCtx.wrongLeader = true
					}
				}

				//只处理请求id单调递增的客户端的请求
				if op.Type == OpPut || op.Type == OpAppend {
					//如果请求id还不存在，说明是第一个请求，如果是递增的请求，则接受变更
					if !existSeq || op.SeqId > prevSeq {
						if op.Type == OpPut {
							kv.kvStore[op.Key] = op.Value
						} else if op.Type == OpAppend {
							if val, exist := kv.kvStore[op.Key]; exist {
								kv.kvStore[op.Key] = val + op.Value
							} else {
								kv.kvStore[op.Key] = op.Value
							}
						}
					} else if existOp {
						opCtx.ignored = true
					}
				} else { //OpPut
					if existOp {
						opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
					}
				}

				//唤醒挂起的RPC
				if existOp {
					close(opCtx.committed)
				}
			}()
		}
	}
}
