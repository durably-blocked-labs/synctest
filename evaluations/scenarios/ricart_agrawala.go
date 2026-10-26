package scenarios

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/evaluations/bubblegun"
	ragate "github.com/shubhaankar/synctest/evaluations/ragate"
	"github.com/shubhaankar/synctest/orchestrator"
)

const RicartAgrawalaGate = "bugs/ricart-agrawala-gate"
const ricartAgrawalaKVAddr = "KV"

func Register(registry *bubblegun.Registry) {
	registry.RegisterScenario(bubblegun.Scenario{
		Name:       RicartAgrawalaGate,
		Processors: 8,
		Generate: func(attempt int) (json.RawMessage, error) {
			return json.Marshal(struct {
				Seed int `json:"seed"`
			}{Seed: attempt})
		},
		Setup: func(json.RawMessage) (func(*orchestrator.Orchestrator), error) {
			return setupRicartAgrawalaGate, nil
		},
	})
}

func setupRicartAgrawalaGate(o *orchestrator.Orchestrator) {
	addresses := []string{"A", "B", "C"}
	transports := ricartAgrawalaCluster(append([]string{ricartAgrawalaKVAddr}, addresses...))
	o.AddNode(transports[ricartAgrawalaKVAddr], func(*testing.T) {
		transports[ricartAgrawalaKVAddr].StartBridge()
		ragate.NewNetworkKVStore(transports[ricartAgrawalaKVAddr]).Serve()
	})

	var done atomic.Int32
	for _, address := range addresses {
		address := address
		transport := transports[address]
		peers := make([]string, 0, len(addresses)-1)
		for _, peer := range addresses {
			if peer != address {
				peers = append(peers, peer)
			}
		}
		o.AddNode(transport, func(t *testing.T) {
			transport.StartBridge()
			node := ragate.NewGateRANode(address, transport, peers)
			node.OnViolation = func(detail string) { t.Error(detail) }
			node.Start()

			client := ragate.NewKVClient(transport, ricartAgrawalaKVAddr)
			node.AcquireLock()
			value := client.Get("counter")
			client.Put("counter", value+1)
			node.ReleaseLock()
			if int(done.Add(1)) == len(addresses) {
				if final := client.Get("counter"); final != len(addresses) {
					t.Errorf("lost update: counter=%d, want %d", final, len(addresses))
				}
			}
		})
	}
}

func ricartAgrawalaCluster(addresses []string) map[string]*ragate.OrchestratorTransport {
	transports := make(map[string]*ragate.OrchestratorTransport, len(addresses))
	for _, address := range addresses {
		transports[address] = ragate.NewOrchestratorTransport(address)
	}
	for _, transport := range transports {
		for _, peer := range transports {
			if transport != peer {
				transport.Connect(peer)
			}
		}
	}
	return transports
}
