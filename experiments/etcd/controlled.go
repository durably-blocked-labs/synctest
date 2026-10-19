// Package etcdexperiment separates the real-files baseline from controlled integration.
package etcdexperiment

// ControlledIntegrationStatus is a fail-closed capability seam, NOT an adapter.
// It must stay unsupported until client and peer byte streams, storage completion,
// time/random inputs and shared-process observations have explicit replay contracts.
func ControlledIntegrationStatus() (supported bool, missing []string) {
	return false, []string{
		"scheduler-owned client gRPC and peer HTTP/stream/snapshot listeners and dialers",
		"declared WAL/bbolt completion and clean-restart storage contract",
		"audited time, randomness, shared globals and cross-bubble ownership",
		"joined reset and strict full-consumption replay qualification",
	}
}
