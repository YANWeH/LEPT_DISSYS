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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

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
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
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
	LEADER = iota
	FOLLOWER
	CANDIDATE
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

	currentTerm int //latest term（第一次启动时初始化为0）
	votedFor    int //记录在当前任期给谁投票了（nuil if none）
	log         []LogEntry

	//所有服务器的易失（volatile）状态信息

	commitIndex int //当前服务器已提交的最大的logentry
	lastApplied int //当前服务器应用到状态机的最大的logentry

	//leader服务器的易失（volatile）状态信息，选举后重置

	nextIndex  []int //记录将要添加的log entry发送到此服务器上的index（初始化为leader最后一个log index+ 1）
	matchIndex []int //记录已复制到此服务器上的最大log entry index

	//所有服务器选举相关状态

	status            int       //服务器所处的状态 0-LEADER 1-FOLLOWER 2-CANDIDATE
	leaderId          int       //当前状态下的leader id，用于客户端在请求非leader时重定向
	lastActiveTime    time.Time //上次活跃时间（刷新时机：收到leader心跳 给其他candidate投票 请求其他节点投票）
	lastBroadcastTime time.Time //自己是leader的话，上次广播时间

	//applyCh chan ApplyMsg //向应用层提交结果的队列
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
		rfLastLogTerm := 0 //记录自身的最大logterm
		if len(rf.log) != 0 {
			rfLastLogTerm = rf.log[len(rf.log)-1].Term
		}
		//candidate的log必须至少比自身的新，否则拒绝投票
		if args.LastLogTerm > rfLastLogTerm || (args.LastLogTerm == rfLastLogTerm && args.LastLogIndex >= len(rf.log)) {
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
	if args.Term < rf.currentTerm {
		return
	}

	//发送者的term更大，则自身变为该term下的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
	}
	rf.leaderId = args.LeaderId

	//接收到了heartbeat，重置选举超时时间
	rf.lastActiveTime = time.Now()

	//如果本地的日志长度少于leader的前一个日志index，说明本地缺少了之前的日志
	if len(rf.log) < args.PrevLogIndex {
		return
	}
	//如果本地日志长度等于leader的前一个日志index，但任期不同
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		return
	}

	//保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i //prevlogindex+1为新log entry的起始位置
		if index > len(rf.log) {
			rf.log = append(rf.log, logEntry)
		} else { //重叠部分
			if rf.log[index-1].Term != logEntry.Term {
				rf.log = rf.log[:index-1]         //删除重叠部分
				rf.log = append(rf.log, logEntry) //添加新log
			}
		}
	}

	rf.persist()

	//更新commitindex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log) < rf.commitIndex {
			rf.commitIndex = len(rf.log)
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index = len(rf.log)
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
	rf.lastActiveTime = time.Now()

	//初始化随机因子
	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//新开一个gorutine执行选举策略
	go rf.electionMonitor()

	//新开一个gorutine仅leader可执行复制log entry或heartbeat
	go rf.appendEntriesMonitor()

	//新开一个goruntine执行applylog，进行日志提交
	go rf.applyLogMonitor(applyCh)

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
					LastLogIndex: len(rf.log),
				}
				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}

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
						rf.nextIndex[i] = len(rf.log) + 1
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

			//如果使用100ms广播一次，当leader与follower之间出现较长的日志差异链的时候，备份时间过长
			//无法通过TestBackup2B这个测试用例
			if currentTime.Sub(rf.lastBroadcastTime) < 50*time.Millisecond {

				return
			}

			//超过100ms后，leader需要立即发送一个heartbeat
			rf.lastBroadcastTime = time.Now()

			DPrintf("--------------------------------------------------")
			DPrintf("Raft peer-%v start append entries\n", rf.me)
			DPrintf("currentTerm:%v LeaderId:%v", rf.currentTerm, rf.leaderId)

			/*type appendResult struct {
				peerId int
				reply  *AppendEntriesReply
			}*/

			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					Entries:      make([]LogEntry, 0),
					PrevLogIndex: rf.nextIndex[peerId] - 1,
				}
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				//leader与follower寻找共同的最大log entry，并将对应的index之后的log entry传递给follower以供更新
				args.Entries = append(args.Entries, rf.log[args.PrevLogIndex:]...)

				go func(id int, args1 AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, &args1, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						defer func() {
							DPrintf("Raft peer-%v finished append entries\n", rf.me)
							DPrintf("currentTerm:%v logIndex:%v nextIndex:%v matchIndex:%v commitIndex:%v\n",
								rf.currentTerm, len(rf.log), rf.nextIndex[id], rf.matchIndex, rf.commitIndex)
							DPrintf("--------------------------------------------------")
						}()

						//如果leader状态已经改变，则不能做任何事
						if args1.Term != rf.currentTerm {
							return
						}

						//自身任期更小，变成follower
						if reply.Term > rf.currentTerm {
							rf.status = FOLLOWER
							rf.leaderId = -1
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							return
						}
						//日志同步成功
						if reply.Success {
							//rf.nextIndex[id] += len(args1.Entries)
							//上面的会造成out of range的错误
							/*猜测：同步失败时，nextIndex[id]会减一，如果成功后使用：
							rf.nextIndex[id] += len(args1.Entries)会造成少一段日志
							必须使用下面的式子，才能完整记录nextIndex*/
							rf.nextIndex[id] = args1.PrevLogIndex + len(args1.Entries) + 1
							rf.matchIndex[id] = rf.nextIndex[id] - 1

							//更新commitIndex，找Raft peers log entry的中位数
							//因为是过半投票，中位数的peer一定满足大多数都拥有此log entry
							sortedMatchIndex := make([]int, 0)
							sortedMatchIndex = append(sortedMatchIndex, len(rf.log))
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
							}
							sort.Ints(sortedMatchIndex)
							newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
							if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex-1].Term == rf.currentTerm {
								rf.commitIndex = newCommitIndex
							}
						} else {
							rf.nextIndex[id] -= 1
							if rf.nextIndex[id] < 1 {
								rf.nextIndex[id] = 1
							}
						}
					}
				}(peerId, args)
			}
		}()
	}
}

func (rf *Raft) applyLogMonitor(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		var appliedMsgs = make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[rf.lastApplied-1].Term,
				})
			}
		}()
		//提交给应用层
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
	}
}
