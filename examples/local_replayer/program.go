// Package localreplayer demonstrates deterministic record and replay of
// goroutine scheduling using synctest bubbles.
//
// The program spawns concurrent goroutines that race to claim work items.
// The scheduling order determines which goroutine gets which item.
// The key property: the same goroutine always receives the same bubble-local
// goroutine ID (BGID) across runs, and replaying a recorded trace produces
// identical scheduling decisions.
package localreplayer

import "sync"

// Run executes a concurrent workload where three goroutines each claim
// one item from a shared list. The scheduling order determines which
// goroutine claims which item.
//
// logf is a printf-style logger (e.g., t.Logf) so goroutines can
// announce their claims from inside the program.
//
// Returns a mapping from goroutine name to claimed item.
func Run(logf func(string, ...any)) map[string]string {
	items := []string{"alpha", "beta", "gamma"}
	var mu sync.Mutex
	idx := 0

	result := make(map[string]string)

	var wg sync.WaitGroup
	for _, name := range []string{"alice", "bob", "carol"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			mu.Lock()
			if idx < len(items) {
				item := items[idx]
				result[name] = item
				logf("%s claimed %s", name, item)
				idx++
			}
			mu.Unlock()
		}(name)
	}

	wg.Wait()
	return result
}
