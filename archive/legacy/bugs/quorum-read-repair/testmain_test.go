package quorumreadrepair

import "runtime/debug"

func init() {
	// The custom synctest runtime can hang in GC during repeated multi-bubble
	// test runs. These tests are scheduler-focused and do not depend on GC.
	debug.SetGCPercent(-1)
}
