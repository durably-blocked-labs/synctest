package orchestrator

import "sync/atomic"

var retiredWorker atomic.Bool

// RetireWorker irreversibly fences these library entry points after an unsafe
// runtime result. It does not dispose goroutines; the process must terminate.
func RetireWorker() { retiredWorker.Store(true) }

// WorkerReusable is false after any unsafe bubble result in this process.
func WorkerReusable() bool { return !retiredWorker.Load() }
