package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool //true为log false为snapshot
	//向应用层（kvserver）提交日志
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	//向应用层（kvserver）安装快照
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

//
// A Go object implementing a single Raft peer.
//

//日志结构体
type LogEntry struct {
	Command interface{} //记录客户端发来的请求（for state machine）
	Term    int         //记录执行当前请求的任期（first index is 1）
}

const (
	LEADER    = "Leader"
	FOLLOWER  = "Follower"
	CANDIDATE = "Candidate"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//所有服务器的持久（persistent）状态信息

	currentTerm       int //latest term（第一次启动时初始化为0）
	votedFor          int //记录在当前任期给谁投票了（nuil if none）
	log               []LogEntry
	lastIncludedIndex int //snapshot最后一个logentry的index，没有snapshot则为0
	lastIncludedTerm  int //snapshot最后一个logentry的term，没有snapshot则无意义

	//所有服务器的易失（volatile）状态信息

	commitIndex int //当前服务器已提交的最大的logentry
	lastApplied int //当前服务器应用到状态机的最大的logentry

	//leader服务器的易失（volatile）状态信息，选举后重置

	nextIndex  []int //记录将要添加的log entry发送到此服务器上的index（初始化为leader最后一个log index+ 1）
	matchIndex []int //记录已复制到此服务器上的最大log entry index

	//所有服务器选举相关状态

	status            string    //服务器所处的状态 0-LEADER 1-FOLLOWER 2-CANDIDATE
	leaderId          int       //当前状态下的leader id，用于客户端在请求非leader时重定向
	lastActiveTime    time.Time //上次活跃时间（刷新时机：收到leader心跳 给其他candidate投票 请求其他节点投票）
	lastBroadcastTime time.Time //自己是leader的话，上次广播时间

	applyCh chan ApplyMsg //向应用层提交结果的队列
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//加锁是为了保证访问该raft peer时的状态不能被改变
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.status == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.raftStateForPersist()
	DPrintf("--------------------------------------------------")
	DPrintf("Raft peer-%v starts persist\n", rf.me)
	DPrintf("currentTerm-%v voteFor-%v log-%v", rf.currentTerm, rf.votedFor, rf.log)
	rf.persister.SaveRaftState(data)
	DPrintf("Raft peer-%v  persist ends\n", rf.me)
	DPrintf("--------------------------------------------------")
}

func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

//
// 			RequestVote RPC
//由candidate调用来获得投票
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int //请求投票时，candidate的term
	CandidateId  int //哪个candidate在请求投票
	LastLogIndex int //candidate最新的log entry的index
	LastLogTerm  int //candidate最新的log entry的trem
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  //最新的term（currentTerm），以供candidate更新自身term
	VoteGranted bool //true表示candidate收到了投票支持
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("--------------------------------------------------")
	DPrintf("handling RequestVote:\n")
	DPrintf("Raft peer-%v and it's Term = %v\n", rf.me, rf.currentTerm)
	DPrintf("Candidate-%v and it's Term = %v\n", args.CandidateId, args.Term)
	DPrintf("LastLogIndex = %v LastLogTerm = %v\n", args.LastLogIndex, args.LastLogTerm)
	DPrintf("Raft VotedFor %v\n", rf.votedFor)

	defer func() {
		DPrintf("Raft peer-%v finished RequestVote\n", rf.me)
		DPrintf("CandidateId:%v VoteGranted:%v", args.CandidateId, reply.VoteGranted)
		DPrintf("--------------------------------------------------")
	}()

	//candidate的term还没有当前raft的大，则拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	//如果candidate的term大于当前raft，则更新term
	if args.Term > rf.currentTerm {
		//开启新term，自身所记录的已过时
		rf.currentTerm = args.Term
		rf.status = FOLLOWER //只能为follower
		rf.votedFor = -1     //重置投票
		rf.leaderId = -1     //重置leader id
	}

	//开始投票
	//一个term内只能投一次，给一个人
	//没投过，或者投票对象就是candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rfLastLogTerm := rf.lastTerm() //记录自身的最大logterm
		//candidate的log必须至少比自身的新，否则拒绝投票
		if args.LastLogTerm > rfLastLogTerm || (args.LastLogTerm == rfLastLogTerm && args.LastLogIndex >= rf.lastIndex()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() //投票成功，重置选举超时时间
		}
	}

	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
//			AppendEntries RPC
//由leader调用，传递log entry或者心跳
//
type AppendEntriesArgs struct {
	Term         int        //leader的term
	LeaderId     int        //传递给follower，以此使客户端请求到follower时可重定向到leader
	PrevLogIndex int        //紧跟在将要添加的log entry之前的index,index+1即为新日志位置
	PrevLogTerm  int        //PrevLogIndex的term
	Entries      []LogEntry //传递给follower的log entry的数组，如果是heartbeat则为空
	LeaderCommit int        //leader中已提交的index
}

type AppendEntriesReply struct {
	Term    int  //最新的term（currentTerm），以供leader更新自身的Term
	Success bool //如果follower包含与PrevLogIndex和PrevLogTerm匹配的条目，则为true

	ConflictTerm  int //follower中与leader冲突的log对应的任期号，如果在对应位置没有log，则返回-1
	ConflictIndex int //follower中，对应任期号为ConflictTerm的第一条log条目的索引号
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("--------------------------------------------------")
	DPrintf("Raft peer-%v handling AppendEntries\n", rf.me)
	DPrintf("LeaderId:%v LeaderTerm:%v currentTerm:%v status:%v\n", args.LeaderId, args.Term, rf.currentTerm, rf.status)

	defer func() {
		DPrintf("Raft peer-%v finished AppendEntries\n", rf.me)
		DPrintf("LeaderId:%v LeaderTerm:%v currentTerm:%v status:%v\n", args.LeaderId, args.Term, rf.currentTerm, rf.status)
		DPrintf("--------------------------------------------------")
	}()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm {
		return
	}

	//发送者的term更大，则自身变为该term下的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}
	rf.leaderId = args.LeaderId

	//接收到了heartbeat，重置选举超时时间
	rf.lastActiveTime = time.Now()

	//如果prevLogIndex在快照内，且不是快照最后一个log，那么只能从index=1开始同步
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex { // prevLogIndex正好等于快照的最后一个log
		if args.PrevLogTerm != rf.lastIncludedTerm { //冲突了，那么从index=1开始同步
			reply.ConflictIndex = 1
			return
		}
	} else { //prevLogIndex在快照之后，那么需要进一步判定
		//如果本地的日志长度少于leader的前一个日志index，说明本地缺少了之前的日志
		if rf.lastIndex() < args.PrevLogIndex {
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
		//如果本地日志长度等于leader的前一个日志index，但任期不同
		if rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
			//从头查找冲突term的第一个位置，可以从尾查找吧？
			for index := rf.lastIncludedIndex + 1; index <= args.PrevLogIndex; index++ {
				if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
	}

	//保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i //prevlogindex+1为新log entry的起始位置
		logPos := rf.index2LogPos(index)
		if index > rf.lastIndex() {
			rf.log = append(rf.log, logEntry)
		} else { //重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          //删除重叠部分
				rf.log = append(rf.log, logEntry) //添加新log
			}
		}
	}

	rf.persist()

	//更新commitindex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
//installsnapshotRPC 由leader调用，发送给follower一个snapshot
//
type InstallSnapshotArgs struct {
	Term              int    //leader的term
	LeaderId          int    //follower可重定向此leader
	LastIncludedIndex int    //snapshot替换了lastIncludedIndex（包括此index）之前的logentry
	LastIncludedTerm  int    //term of lastIncludedIndex
	Offset            int    //chunk在快照文件中位置的字节偏移量
	Data              []byte //snapshot chunk的字节数组，从offset开始
	Done              bool   //true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

//安装快照RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] installSnapshot starts, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	//发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}

	//认识新leader
	rf.leaderId = args.LeaderId
	//刷新活跃时间
	rf.lastActiveTime = time.Now()

	//leader快照不如本地长，那么直接忽略此快照请求
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else { //leader快照更长
		if args.LastIncludedIndex < rf.lastIndex() { //做快照的index之后还有日志，判断是否需要截断
			if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				rf.log = make([]LogEntry, 0) //term冲突，丢掉快照之后的所有日志
			} else { //丢弃快照之前的log
				leftLog := make([]LogEntry, rf.lastIndex()-args.LastIncludedIndex)
				copy(leftLog, rf.log[rf.index2LogPos(args.LastIncludedIndex)+1:])
				rf.log = leftLog
			}
		} else {
			//快照比本地的长，则日志清空
			rf.log = make([]LogEntry, 0)
		}
	}
	//更新快照位置
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	//持久化raft state和snapshot
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), args.Data)
	//snapshot提交给应用层
	rf.installSnapshotToApplication()
	DPrintf("RaftNode[%d] installSnapshot ends, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))
}

//将snapshot同步给应用层
func (rf *Raft) installSnapshotToApplication() {
	//同步给应用层的快照
	applyMsg := &ApplyMsg{
		CommandValid:      false,
		Snapshot:          rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	//快照部分已经提交给应用层了，所有后续提交日志后移至快照之后
	rf.lastApplied = rf.lastIncludedIndex

	DPrintf("RaftNode[%d] installSnapshotToApplication, snapshotSize[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, len(applyMsg.Snapshot), applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm)
	rf.applyCh <- *applyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != LEADER {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = rf.lastIndex()
	term = rf.currentTerm

	rf.persist()

	DPrintf("Raft peer-%v appending command! logIndxe:%v currentTerm:%v", rf.me, index, term)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//向应用层安装快照
	rf.installSnapshotToApplication()

	//新开一个gorutine执行选举策略
	go rf.electionMonitor()

	//新开一个gorutine仅leader可执行复制log entry或heartbeat
	go rf.appendEntriesMonitor()

	//新开一个goruntine执行applylog，进行日志提交
	go rf.applyLogMonitor()

	return rf
}

//选举的逻辑代码
func (rf *Raft) electionMonitor() {
	//检查raft是否被killed，如果没有则：
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			currentTime := time.Now()

			//随机化超时时间 200ms~350ms
			randTimeOut := time.Duration(200+rand.Int31n(150)) * time.Millisecond
			//距离上次接收到leader信息的时间差
			elapses := currentTime.Sub(rf.lastActiveTime)

			if rf.status == FOLLOWER {
				if elapses >= randTimeOut {
					rf.status = CANDIDATE
					DPrintf("Raft peer-%v status: FOLLOWER -> CANDIDATE\n", rf.me)
				}
			}

			//成为了candidate，请求投票
			if rf.status == CANDIDATE && elapses >= randTimeOut {
				rf.lastActiveTime = currentTime //重置选举超时时间
				rf.currentTerm += 1             //开启新term
				rf.votedFor = rf.me             //投票给自己

				rf.persist()

				//向其他raft peer请求投票
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIndex(),
				}
				args.LastLogTerm = rf.lastTerm()

				rf.mu.Unlock()

				DPrintf("--------------------------------------------------")
				DPrintf("Raft peer-%v start request others for vote\n", rf.me)
				DPrintf("currentTerm:%v LastLogIndex:%v LastLogTerm:%v\n", args.Term, args.LastLogIndex, args.LastLogTerm)

				type voteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				voteCount := 1   //收到的投票总数，初始有自身1票
				finishCount := 1 //收到的应答数
				//使用channel来接收其他raft的应答信息
				voteResultChan := make(chan *voteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					//并行发送投票请求到每个raft peer上
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &voteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &voteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}
				lastestTerm := 0 //记录从其他raft收到的Term中是否有更新的term

				//循环读取voteResultChan，以此获得应答信息
				for {
					select {
					case voteRes := <-voteResultChan:
						finishCount += 1
						if voteRes.resp != nil {
							if voteRes.resp.VoteGranted {
								voteCount += 1
							}
							if voteRes.resp.Term > lastestTerm {
								lastestTerm = voteRes.resp.Term
							}
						}
						//得到大多数投票后离开
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("Raft peer-%v finished request vote\n", rf.me)
					DPrintf("responseNum:%v voteNum:%v status:%v lastestTerm:%v currentTerm:%v\n",
						finishCount, voteCount, rf.status, lastestTerm, rf.currentTerm)
					DPrintf("--------------------------------------------------")
				}()

				//如果状态改变，则本次投票结果作废
				if rf.status != CANDIDATE {
					return
				}

				//如果candidate的term比某个raft小，则切回follower
				if lastestTerm > rf.currentTerm {
					rf.status = FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = lastestTerm
					rf.votedFor = -1

					rf.persist()

					return
				}

				//获得大多数投票
				if voteCount > len(rf.peers)/2 {
					rf.status = LEADER
					rf.leaderId = rf.me

					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}

					rf.lastBroadcastTime = time.Unix(0, 0) //立即执行appendEntries（发送心跳包）
					return
				}
			}
		}()
	}
}

//更新已提交的log index
func (rf *Raft) updateCommitIndex() {
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	//如果index属于snapshot范围，那么不用检查term，因为snapshot一定是集群提交的
	//必须要满足当前任期内的日志，才能提交，不能提前提交之前任期的日志
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.lastIncludedIndex || rf.log[rf.index2LogPos(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}
	DPrintf("RaftNode[%d] updateCommitIndex, commitIndex[%d] matchIndex[%v]", rf.me, rf.commitIndex, sortedMatchIndex)
}

func (rf *Raft) doAppendEntries(peerId int) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]LogEntry, 0),
		PrevLogIndex: rf.nextIndex[peerId] - 1,
	}
	//如果pervLogIndex是leader快照的最后一条log，那么取快照的最后一个term
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else { //否则一定是在快照之后的log部分
		args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex+1):]...)

	DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
		rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)

	go func() {
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			defer func() {
				DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			}()

			//如果不是RPC前的leader状态，那么什么也不做
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { //变成follower
				rf.status = FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			if reply.Success {
				//rf.nextIndex[id] += len(args.Entries)
				//上面的会造成out of range的错误
				/*猜测：RPC期间无锁，可能相关状态被其他RPC改变
				因此，得根据发出RPC请求时得状态做更新
				而不能直接对nextIndex做相对加减*/
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				rf.updateCommitIndex()
			} else { //同步失败，需要回退
				prevNextIndex := rf.nextIndex[peerId]

				//造成冲突的位置上（prevLogIndex），follower有对应log，但term不同
				if reply.ConflictTerm != -1 {
					conflictTermIndex := -1 //找到leader中的第一个conflictTerm相同的索引号
					for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
						if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 { //leader中存在与follower中conflictTerm相同的任期号
						rf.nextIndex[peerId] = conflictTermIndex
					} else { //leader中不存在与follower中conflictTerm相同的任期号
						rf.nextIndex[peerId] = reply.ConflictIndex
					}
				} else { //follower的prevLogIndex位置没有日志
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
				DPrintf("Raft peer-%v back-off nextIndex, peer-%v prevNextIndex-%v nextIndex-%v",
					rf.me, peerId, prevNextIndex, rf.nextIndex[peerId])
			}
		}
	}()
}

func (rf *Raft) doInstallSnapshot(peerId int) {
	DPrintf("RaftNode[%d] doInstallSnapshot starts, leaderId[%d] peerId[%d]\n", rf.me, rf.leaderId, peerId)

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}

	reply := InstallSnapshotReply{}

	go func() {
		if ok := rf.sendInstallSnapshot(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//如果不是RPC前的leader状态，则什么也不做
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { //变成follower
				rf.status = FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			rf.nextIndex[peerId] = rf.lastIndex() + 1      //重新从尾部同步log
			rf.matchIndex[peerId] = args.LastIncludedIndex //已同步的位置
			rf.updateCommitIndex()
			DPrintf("RaftNode[%d] doInstallSnapshot ends, leaderId[%d] peerId[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]\n", rf.me, rf.leaderId, peerId, rf.nextIndex[peerId],
				rf.matchIndex[peerId], rf.commitIndex)
		}
	}()
}

//leader传递消息的逻辑代码
func (rf *Raft) appendEntriesMonitor() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//仅leader可发送
			if rf.status != LEADER {
				return
			}

			//100ms广播一次
			currentTime := time.Now()
			if currentTime.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}

			//超过100ms后，leader需要立即发送一个heartbeat
			rf.lastBroadcastTime = time.Now()

			DPrintf("--------------------------------------------------")
			DPrintf("Raft peer-%v start append entries\n", rf.me)
			DPrintf("currentTerm:%v LeaderId:%v", rf.currentTerm, rf.leaderId)

			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				//如果nextIndex在leader的snapshot内，那么直接同步snapshot
				if rf.nextIndex[peerId] <= rf.lastIncludedIndex {
					rf.doInstallSnapshot(peerId)
				} else {
					rf.doAppendEntries(peerId)
				}
			}
		}()
	}
}

func (rf *Raft) applyLogMonitor() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
		}

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
				noMore = false
			}
		}()
	}
}

//最后的index
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

//最后的Term
func (rf *Raft) lastTerm() (lastLogTerm int) {
	lastLogTerm = rf.lastIncludedTerm //for snapshot
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

//日志index转化成log数组下标
func (rf *Raft) index2LogPos(index int) (pos int) {
	return index - rf.lastIncludedIndex - 1
}

//日志是否需要压缩
func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize() >= logSize
}

//保存快照，截断log
func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//已经有更大的snapshot了
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	// 快照的当前元信息
	DPrintf("RafeNode[%d] TakeSnapshot begins, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 要压缩的日志长度
	compactLogLen := lastIncludedIndex - rf.lastIncludedIndex

	//更新快照元信息
	rf.lastIncludedTerm = rf.log[rf.index2LogPos(lastIncludedIndex)].Term
	rf.lastIncludedIndex = lastIncludedIndex

	//压缩日志
	afterLog := make([]LogEntry, len(rf.log)-compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	//把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)

	DPrintf("RafeNode[%d] TakeSnapshot ends, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)
}
