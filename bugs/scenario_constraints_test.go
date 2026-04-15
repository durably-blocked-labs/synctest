package bugs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewBugScenariosAvoidTimeSleep(t *testing.T) {
	t.Parallel()

	packages := []string{
		"ra-lease-expiry",
		"ra-quorum-cascade",
		"ra-ghost-grant",
		"ra-priority-inversion",
		"ra-deferred-storm",
		"ra-quorum-eclipse",
	}

	files := []string{"scenario.go", "lock.go"}

	for _, pkg := range packages {
		for _, name := range files {
			path := filepath.Join(pkg, name)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			if strings.Contains(string(data), "time.Sleep(") {
				t.Fatalf("%s uses time.Sleep; scenarios must use orchestrator-visible coordination instead", path)
			}
		}
	}
}
