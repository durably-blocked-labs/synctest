package orchestrator

import (
	"math/rand"
	"testing"
)

// Random picks uniformly at random for both global and local decisions.
type Random struct {
	Seed int64

	rng   *rand.Rand
	count int
}

func (r *Random) BeforeRun() bool {
	r.rng = rand.New(rand.NewSource(r.Seed + int64(r.count)))
	return true
}

func (r *Random) Decide(dp DecisionPoint) int {
	if dp.N() <= 1 {
		return 0
	}
	return r.rng.Intn(dp.N())
}

func (r *Random) AfterRun(result RunResult) {
	r.count++
}

// Count returns the number of runs completed.
func (r *Random) Count() int { return r.count }

// ─── Convenience ───

type RandomOption func(*randomOpts)
type randomOpts struct {
	seed, maxRuns int64
	observer      RunObserver
}

func RandomSeed(s int64) RandomOption  { return func(o *randomOpts) { o.seed = s } }
func RandomMaxRuns(n int) RandomOption { return func(o *randomOpts) { o.maxRuns = int64(n) } }
func RandomWithObserver(fn RunObserver) RandomOption {
	return func(o *randomOpts) { o.observer = fn }
}

// ExploreRandom samples schedules with uniform random G+L choices.
func (o *Orchestrator) ExploreRandom(t *testing.T, setup func(*Orchestrator), opts ...RandomOption) bool {
	t.Helper()
	cfg := randomOpts{seed: 1, maxRuns: 100}
	for _, opt := range opts {
		opt(&cfg)
	}
	algo := &Random{Seed: cfg.seed}
	var eopts []ExploreOption
	if cfg.maxRuns > 0 {
		eopts = append(eopts, GlobalMaxRuns(int(cfg.maxRuns)))
	}
	if cfg.observer != nil {
		eopts = append(eopts, WithObserver(cfg.observer))
	}
	return o.ExploreWith(t, setup, algo, eopts...).Failed == 0
}
