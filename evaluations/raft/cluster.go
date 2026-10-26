// Package rafteval provides reusable HashiCorp Raft evaluation workloads.
package rafteval

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/orchestrator"
	"github.com/shubhaankar/synctest/rafttest"
)

// NodeTest runs inside each Raft node's bubble after the node has started.
type NodeTest func(t *testing.T, node *Node)

// Node exposes one running Raft node to an evaluation workload.
type Node struct {
	ID   raft.ServerID
	Raft *raft.Raft
	FSM  *RecordingFSM
}

// SetupOption configures a cluster setup.
type SetupOption func(*setupConfig)

type setupConfig struct {
	configure func(*raft.Config)
	seed      *int64
}

// WithConfig customizes each node's Raft configuration.
func WithConfig(configure func(*raft.Config)) SetupOption {
	return func(cfg *setupConfig) {
		cfg.configure = configure
	}
}

// WithSeed resets math/rand before every execution. Callers using Go 1.24 or
// newer must enable GODEBUG=randseednop=0 for this option to take effect.
func WithSeed(seed int64) SetupOption {
	return func(cfg *setupConfig) {
		cfg.seed = &seed
	}
}

// NewClusterSetup returns an ExploreWith setup function. Every execution gets
// fresh transports, stores, FSMs, and real HashiCorp Raft instances.
func NewClusterSetup(size int, test NodeTest, opts ...SetupOption) func(*orchestrator.Orchestrator) {
	return NewClusterSetupFactory(size, func() NodeTest { return test }, opts...)
}

// NewClusterSetupFactory creates fresh workload state before every execution.
func NewClusterSetupFactory(size int, factory func() NodeTest, opts ...SetupOption) func(*orchestrator.Orchestrator) {
	if size < 1 {
		panic("rafteval: cluster size must be positive")
	}
	if factory == nil {
		panic("rafteval: node test factory must not be nil")
	}

	cfg := setupConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	return func(orch *orchestrator.Orchestrator) {
		if cfg.seed != nil {
			rand.Seed(*cfg.seed) //nolint:staticcheck
		}

		configuration := raft.Configuration{Servers: make([]raft.Server, size)}
		transports := make([]*rafttest.RaftTransport, size)
		for i := range size {
			address := raft.ServerAddress(fmt.Sprintf("node%d", i+1))
			configuration.Servers[i] = raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(address),
				Address:  address,
			}
			transports[i] = rafttest.NewRaftTransport(address)
		}
		for i := range transports {
			for j := range transports {
				if i != j {
					transports[i].Connect(transports[j])
				}
			}
		}

		test := factory()
		if test == nil {
			panic("rafteval: node test factory returned nil")
		}
		run := newClusterRun(size)
		for i, transport := range transports {
			server := configuration.Servers[i]
			orch.AddNode(transport, func(t *testing.T) {
				runNode(t, run, server.ID, configuration, transport, test, cfg.configure)
			})
		}
	}
}

type clusterRun struct {
	size           int32
	finished       atomic.Int32
	raftStopped    atomic.Int32
	allDone        chan struct{}
	allRaftStopped chan struct{}
}

func newClusterRun(size int) *clusterRun {
	return &clusterRun{
		size:           int32(size),
		allDone:        make(chan struct{}),
		allRaftStopped: make(chan struct{}),
	}
}

func (r *clusterRun) waitForRaftShutdown() {
	if r.raftStopped.Add(1) == r.size {
		close(r.allRaftStopped)
	}
	synctest.ExternalWait(func() {
		<-r.allRaftStopped
	})
}

func (r *clusterRun) waitForWorkloads() {
	if r.finished.Add(1) == r.size {
		close(r.allDone)
	}
	synctest.ExternalWait(func() {
		<-r.allDone
	})
}

func runNode(
	t *testing.T,
	run *clusterRun,
	id raft.ServerID,
	configuration raft.Configuration,
	transport *rafttest.RaftTransport,
	test NodeTest,
	configure func(*raft.Config),
) {
	transport.StartBridge()

	var instance *raft.Raft
	defer func() {
		run.waitForWorkloads()
		if instance != nil {
			if err := instance.Shutdown().Error(); err != nil {
				t.Errorf("shutdown raft %s: %v", id, err)
			}
		}
		run.waitForRaftShutdown()
		if err := transport.Close(); err != nil {
			t.Errorf("close transport %s: %v", id, err)
		}
	}()

	store := raft.NewInmemStore()
	snapshotStore := raft.NewDiscardSnapshotStore()
	fsm := NewRecordingFSM()
	config := defaultConfig(id)
	if configure != nil {
		configure(config)
	}
	config.LocalID = id

	if err := raft.BootstrapCluster(
		config,
		store,
		store,
		snapshotStore,
		transport,
		configuration,
	); err != nil {
		t.Fatalf("bootstrap raft %s: %v", id, err)
	}

	var err error
	instance, err = raft.NewRaft(
		config,
		fsm,
		store,
		store,
		snapshotStore,
		transport,
	)
	if err != nil {
		t.Fatalf("start raft %s: %v", id, err)
	}

	test(t, &Node{
		ID:   id,
		Raft: instance,
		FSM:  fsm,
	})
}

func defaultConfig(id raft.ServerID) *raft.Config {
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond
	config.LocalID = id
	config.Logger = nil
	config.SnapshotThreshold = 1 << 20
	return config
}

// WaitForLeader waits until this node observes a leader.
func (n *Node) WaitForLeader(t *testing.T, timeout time.Duration) raft.ServerAddress {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if leader := n.Raft.Leader(); leader != "" {
			return leader
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("node %s: timed out waiting for leader", n.ID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// IsLeader reports whether this node is currently the Raft leader.
func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

// Apply submits a command and bounds the full commit-and-apply operation.
func (n *Node) Apply(command []byte, timeout time.Duration) (uint64, error) {
	future := n.Raft.Apply(command, 0)
	done := make(chan error, 1)
	go func() {
		done <- future.Error()
	}()

	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	select {
	case err := <-done:
		return future.Index(), err
	case <-timer:
		return 0, fmt.Errorf("node %s: apply timed out after %s", n.ID, timeout)
	}
}

// WaitForCommands waits until this node's FSM has applied at least count commands.
func (n *Node) WaitForCommands(t *testing.T, count int, timeout time.Duration) []AppliedEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		history := n.FSM.History()
		if len(history) >= count {
			return history
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("node %s: applied %d commands, want at least %d", n.ID, len(history), count)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
