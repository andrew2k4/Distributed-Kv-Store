package raft

import (
	"context"
	"distributed_kv_store/internal/pb"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type Raft struct {
	selfID string
	statePath string
	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry
	role  		Role
	commitIndex int64
	lastApplied int64
	nextIndex 	[]int64
	matchIndex 	[]int64
	timer *time.Timer
	ticker *time.Ticker
	counter int64
	peerClients map[string]pb.KvStoreClient
	peers []Peer
	mu sync.Mutex
}

type Peer struct{
	ID string
	Address string
}


func NewRaft(selfID string, peers []Peer) (*Raft, error) {
	r := &Raft{
		selfID:      selfID,
		statePath:   fmt.Sprintf("raft_state_%s.json", selfID),
		timer: time.NewTimer(time.Duration(rand.Intn(151) + 150) * time.Millisecond),
		peers:       peers,
		peerClients: make(map[string]pb.KvStoreClient, len(peers)),
	}

	if err := r.loadState(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	for _, p := range peers {
		conn, err := grpc.NewClient(p.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		r.peerClients[p.ID] = pb.NewKvStoreClient(conn)
	}

	return r, nil
}

// Run starts the background loop driving election timeouts. It never returns.
func (r *Raft) Run() {
	for {
		r.becomeCandidate()
		time.Sleep(10 * time.Millisecond)
	}
}

type LogEntry struct {
	Term  int64
	Index int64
	Op    string
	Key   string
	Value string
}

func (r *Raft) persist() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.persistLocked()
}

// persistLocked assumes r.mu is already held by the caller.
func (r *Raft) persistLocked() error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}

	tmpPath := r.statePath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, r.statePath)
}


func(r *Raft) loadState() error {
	data, err := os.ReadFile(r.statePath)
	if err != nil {
		return err
	}
	
	err = json.Unmarshal(data, r)
	return err
}

func(r *Raft) becomeCandidate () {
	select{
	case <- r.timer.C:
		r.mu.Lock()
		if r.role == Leader {
			r.mu.Unlock()
			r.timer.Reset(time.Duration(rand.Intn(151) + 150) * time.Millisecond)
			return
		}
		r.role = Role(Candidate)
		r.CurrentTerm++
		r.VotedFor = r.selfID // i would change it later
		r.counter = 1
		r.mu.Unlock()
		log.Printf("[%s] election timeout, becoming candidate for term %d", r.selfID, r.CurrentTerm)

		lastLogIndex := int64(0)
		lastLogTerm := int64(0)
		if len(r.Log) == 0 {
			lastLogIndex = 0
			lastLogTerm = 0
		}else{
			lastLogIndex = r.Log[len(r.Log) -1 ].Index
			lastLogTerm = r.Log[len(r.Log) -1 ].Term
		}

		requestVoteRequest := &pb.RequestVoteRequest{
			Term: r.CurrentTerm,
			CandidateId: r.selfID,
			LastLogIndex: lastLogIndex,
			LastLogTerm: lastLogTerm,
		}

		for _ ,value := range r.peerClients {
			r.mu.Lock()
			
			if r.role == Follower{
				r.mu.Unlock()
				break
			}
			r.mu.Unlock()
			go r.requestVote(value,requestVoteRequest)
		}

		r.persist()
		r.mu.Lock()
		r.timer.Reset(time.Duration(rand.Intn(151) + 150) * time.Millisecond)
		r.mu.Unlock()
		return
	default:
		
	}

}

func(r *Raft) requestVote(peer pb.KvStoreClient,requestVoteRequest *pb.RequestVoteRequest)  {
	ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
	defer cancel()


	resp , err:= peer.RequestVote(ctx,requestVoteRequest)

	if err != nil {
		return
	}

	r.mu.Lock()
	if(resp.Term > r.CurrentTerm){
		r.role = Follower
	}

	if resp.VoteGranted {
		r.counter ++
	}
	r.mu.Unlock()
	r.mu.Lock()
	if r.role == Candidate && r.counter > int64((1 + len(r.peerClients)) / 2) {
		r.role = Leader
		r.ticker = time.NewTicker(50 * time.Millisecond)
		r.mu.Unlock()
		go r.appendEntries(nil)

		go func() {

			for range r.ticker.C {
				go r.appendEntries(nil)
				r.mu.Lock()
				if(r.role != Leader){
					r.ticker.Stop()
					r.mu.Unlock()
					break
		        }
				r.mu.Unlock()
			}
		}()
		

		log.Printf("[%s] won election for term %d with %d votes, becoming leader", r.selfID, r.CurrentTerm, r.counter)
		return
	}
	r.mu.Unlock()
}

func (r *Raft) Vote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.CurrentTerm {
		return &pb.RequestVoteResponse{
			Term:        r.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	changed := false
	if req.Term > r.CurrentTerm {
		r.CurrentTerm = req.Term
		r.VotedFor = ""
		r.role = Follower
		changed = true
	}

	lastLogIndex := int64(0)
	lastLogTerm := int64(0)
	if len(r.Log) > 0 {
		lastLogIndex = r.Log[len(r.Log)-1].Index
		lastLogTerm = r.Log[len(r.Log)-1].Term
	}

	logIsUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	canVote := r.VotedFor == "" || r.VotedFor == req.CandidateId
	granted := canVote && logIsUpToDate

	if granted {
		r.VotedFor = req.CandidateId
		changed = true
	}

	if changed {
		if err := r.persistLocked(); err != nil {
			return nil, err
		}
	}

	if granted {
		r.timer.Reset(time.Duration(rand.Intn(151)+150) * time.Millisecond)
	}

	log.Printf("[%s] vote request from %s (term %d): granted=%v", r.selfID, req.CandidateId, req.Term, granted)

	return &pb.RequestVoteResponse{
		Term:        r.CurrentTerm,
		VoteGranted: granted,
	}, nil
}

func (r *Raft) appendEntries(req *pb.AppendEntriesRequest) {
	
	for _, value := range r.peerClients {
		
		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()
		if req != nil {
			// todo after
		}else{
			r.mu.Lock()
			req := &pb.AppendEntriesRequest{
				Term: r.CurrentTerm,
				LeaderId: r.selfID,
				PrevLogIndex: 0,
				PrevLogTerm: 0,
				Entries: []*pb.LogEntry{},
				LeaderCommit: r.commitIndex,
			}
			r.mu.Unlock()
			_ , err := value.AppendEntries(ctx, req)
			if err != nil {
				log.Printf("[%s] error appending entries to %s: %v", r.selfID, value, err)
			}
		}

	}
}


func (r *Raft) Append(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if(req.Term < r.CurrentTerm){
		return &pb.AppendEntriesResponse{
			Term: r.CurrentTerm,
			Success: false,
			ConflictIndex: 0,
			ConflictTerm: 0,
		}, nil
	}

	r.role = Follower
	r.timer.Reset(time.Duration(rand.Intn(151) + 150) * time.Millisecond)
	
	if(req.Term > r.CurrentTerm){
		r.CurrentTerm = req.Term
		r.persistLocked()
	}

	return &pb.AppendEntriesResponse{
		Term: r.CurrentTerm,
		Success: true,
		ConflictIndex: 0,
		ConflictTerm: 0,
	}, nil
	
}



