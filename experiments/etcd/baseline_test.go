package etcdexperiment

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type observation struct {
	Node                               string
	Incarnation                        int
	Member, Leader, Applied, Committed uint64
	Directory                          string
}

// This is REAL loopback, gRPC/HTTP, WAL and bbolt I/O, not a deterministic
// Bubbles simulation. A clean Close/reopen is not an abrupt machine crash.
func TestEmbeddedThreeNodeKVStopRestartBaseline(t *testing.T) {
	root := t.TempDir()
	var servers [3]*embed.Etcd
	var clients [3]*clientv3.Client
	var configs [3]*embed.Config
	var reservations []net.Listener
	defer func() {
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
		for _, server := range servers {
			if server != nil {
				server.Close()
				<-server.Server.StopNotify()
			}
		}
		for _, listener := range reservations {
			_ = listener.Close()
		}
	}()
	reserve := func() url.URL {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		reservations = append(reservations, listener)
		return url.URL{Scheme: "http", Host: listener.Addr().String()}
	}
	var cluster []string
	for i := range configs {
		config := embed.NewConfig()
		config.Name = fmt.Sprintf("node-%d", i)
		config.Dir = filepath.Join(root, config.Name)
		config.ListenPeerUrls = []url.URL{reserve()}
		config.AdvertisePeerUrls = config.ListenPeerUrls
		config.ListenClientUrls = []url.URL{reserve()}
		config.AdvertiseClientUrls = config.ListenClientUrls
		config.InitialClusterToken = filepath.Base(root)
		config.Logger = "zap"
		config.LogLevel = "error"
		config.LogOutputs = []string{"stderr"}
		configs[i] = config
		cluster = append(cluster, config.Name+"="+config.AdvertisePeerUrls[0].String())
	}
	start := func(i int) {
		server, err := embed.StartEtcd(configs[i])
		if err != nil {
			t.Fatalf("infrastructure: start node %d (port reservation/rebind can race): %v", i, err)
		}
		servers[i] = server
	}
	for i, config := range configs {
		config.InitialCluster = strings.Join(cluster, ",")
		_ = reservations[2*i].Close()
		_ = reservations[2*i+1].Close()
		start(i)
	}
	ready := func(i int) {
		select {
		case <-servers[i].Server.ReadyNotify():
		case err := <-servers[i].Err():
			t.Fatalf("infrastructure: node %d: %v", i, err)
		case <-time.After(15 * time.Second):
			t.Fatalf("inconclusive: node %d readiness watchdog", i)
		}
	}
	connect := func(i int) {
		client, err := clientv3.New(clientv3.Config{Endpoints: []string{configs[i].AdvertiseClientUrls[0].String()}, DialTimeout: 3 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client
	}
	observe := func(i, incarnation int) observation {
		server := servers[i].Server
		obs := observation{configs[i].Name, incarnation, uint64(server.MemberID()), uint64(server.Leader()), server.AppliedIndex(), server.CommittedIndex(), configs[i].Dir}
		// Never use process-global expvar/Prometheus collectors as node truth.
		t.Logf("instance observation: %+v", obs)
		return obs
	}
	for i := range servers {
		ready(i)
		connect(i)
		observe(i, 1)
	}
	withContext := func(f func(context.Context)) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		f(ctx)
	}
	var revision int64
	withContext(func(ctx context.Context) {
		response, err := clients[0].Put(ctx, "register", "alpha")
		if err != nil {
			t.Fatalf("inconclusive: Put error can be ambiguous: %v", err)
		}
		revision = response.Header.Revision
	})
	withContext(func(ctx context.Context) {
		response, err := clients[1].Txn(ctx).If(clientv3.Compare(clientv3.Value("register"), "=", "alpha")).Then(clientv3.OpPut("register", "beta")).Else(clientv3.OpGet("register")).Commit()
		if err != nil {
			t.Fatalf("inconclusive: Txn error can be ambiguous: %v", err)
		}
		if !response.Succeeded || response.Header.Revision <= revision {
			t.Fatal("KV oracle: successful Put then CAS must advance alpha to beta")
		}
		revision = response.Header.Revision
	})
	check := func(i int, expected string) {
		withContext(func(ctx context.Context) {
			response, err := clients[i].Get(ctx, "register") // Default linearizable, not serializable.
			if err != nil {
				t.Fatalf("inconclusive: read: %v", err)
			}
			if len(response.Kvs) != 1 || string(response.Kvs[0].Value) != expected || response.Header.Revision < revision || response.Kvs[0].ModRevision != revision {
				t.Fatalf("bounded sequential KV oracle on node %d: got %+v expected %q at revision %d", i, response, expected, revision)
			}
			if response.Header.MemberId != uint64(servers[i].Server.MemberID()) {
				t.Fatal("endpoint observation attributed to wrong instance")
			}
		})
	}
	for i := range servers {
		check(i, "beta")
	}
	follower := -1
	for i, server := range servers {
		if server.Server.Leader() != 0 && server.Server.MemberID() != server.Server.Leader() {
			follower = i
			break
		}
	}
	if follower < 0 {
		t.Fatal("inconclusive: no follower to stop")
	}
	before := observe(follower, 1)
	_ = clients[follower].Close()
	clients[follower] = nil
	servers[follower].Close()
	<-servers[follower].Server.StopNotify()
	servers[follower] = nil
	live := (follower + 1) % 3
	withContext(func(ctx context.Context) {
		response, err := clients[live].Put(ctx, "register", "gamma")
		if err != nil {
			t.Fatalf("inconclusive: two-live-node Put: %v", err)
		}
		if response.Header.Revision <= revision {
			t.Fatal("KV oracle: revision failed to advance")
		}
		revision = response.Header.Revision
	})
	configs[follower].ClusterState = embed.ClusterStateFlagExisting
	start(follower)
	ready(follower)
	connect(follower)
	for i := range servers {
		check(i, "gamma")
	}
	after := observe(follower, 2)
	if before.Member != after.Member || before.Directory != after.Directory || after.Applied < before.Applied {
		t.Fatalf("clean restart identity/index mismatch: before=%+v after=%+v", before, after)
	}
}
