package orchestrator

import (
	"math/rand"
	"sort"
	"testing"
)

const pctBasePriority = 1 << 30

// ─── PCT Algorithm ───

// PCT implements combined global + local PCT scheduling.
//
// Every schedulable entity (goroutine or message class) gets a random
// priority. At each decision point — global delivery OR local goroutine
// scheduling — the entity with the lowest priority number wins.
// At d-1 randomly sampled change points, the winning entity is demoted.
//
// Entities are identified by Alt.ID:
//   - Global: "msg:A→B(Request)" (message class)
//   - Local:  "B5" (bubble goroutine ID)
type PCT struct {
	Depth    int   // priority changes = Depth-1 (default 2)
	MaxSteps int   // step range for sampling change points (default 256)
	Seed     int64 // base seed, incremented per run

	rng          *rand.Rand
	priority     map[string]int // Alt.ID → priority
	nextLow      int
	changePoints map[int]struct{}
	step         int // current step within the run (tracks G+L)
	count        int
}

func (p *PCT) BeforeRun() bool {
	if p.Depth == 0 {
		p.Depth = 2
	}
	if p.MaxSteps == 0 {
		p.MaxSteps = 256
	}

	runSeed := p.Seed + int64(p.count)
	p.rng = rand.New(rand.NewSource(runSeed))
	p.priority = make(map[string]int)
	p.nextLow = pctBasePriority
	p.step = 0

	// Sample d-1 change points from [0, MaxSteps).
	nChanges := p.Depth - 1
	if nChanges < 0 {
		nChanges = 0
	}
	if nChanges > p.MaxSteps {
		nChanges = p.MaxSteps
	}
	p.changePoints = make(map[int]struct{}, nChanges)
	if nChanges > 0 {
		perm := p.rng.Perm(p.MaxSteps)
		for i := 0; i < nChanges; i++ {
			p.changePoints[perm[i]] = struct{}{}
		}
	}

	return true
}

// Decide picks the alternative with the lowest priority (both G and L).
func (p *PCT) Decide(dp DecisionPoint) int {
	if dp.N() <= 1 {
		p.step++
		return 0
	}

	chosen := 0
	chosenPri := p.priorityOf(dp.Alts[0].ID)
	for i := 1; i < dp.N(); i++ {
		pri := p.priorityOf(dp.Alts[i].ID)
		if pri < chosenPri {
			chosen = i
			chosenPri = pri
		}
	}

	// At change points, demote the winner.
	if _, ok := p.changePoints[p.step]; ok {
		p.priority[dp.Alts[chosen].ID] = p.nextLow
		p.nextLow++
	}

	p.step++
	return chosen
}

func (p *PCT) AfterRun(result RunResult) {
	p.count++
}

func (p *PCT) priorityOf(id string) int {
	if pri, ok := p.priority[id]; ok {
		return pri
	}
	pri := p.rng.Intn(pctBasePriority)
	p.priority[id] = pri
	return pri
}

// Count returns the number of runs completed.
func (p *PCT) Count() int { return p.count }

// SortedChangePoints returns the sampled change point steps.
func (p *PCT) SortedChangePoints() []int {
	pts := make([]int, 0, len(p.changePoints))
	for step := range p.changePoints {
		pts = append(pts, step)
	}
	sort.Ints(pts)
	return pts
}

// ─── Random Algorithm ───

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

func (r *Random) Count() int { return r.count }

// ─── Convenience methods ───

type PCTOption func(*pctOpts)
type pctOpts struct {
	depth, maxSteps, maxRuns int
	seed                     int64
	observer                 RunObserver
}

func PCTDepth(d int) PCTOption          { return func(o *pctOpts) { o.depth = d } }
func PCTMaxSteps(n int) PCTOption       { return func(o *pctOpts) { o.maxSteps = n } }
func PCTSeed(s int64) PCTOption         { return func(o *pctOpts) { o.seed = s } }
func PCTMaxRuns(n int) PCTOption        { return func(o *pctOpts) { o.maxRuns = n } }
func PCTWithObserver(fn RunObserver) PCTOption { return func(o *pctOpts) { o.observer = fn } }

// ExplorePCT samples schedules using combined G+L PCT priorities.
func (o *Orchestrator) ExplorePCT(t *testing.T, setup func(*Orchestrator), opts ...PCTOption) bool {
	t.Helper()
	cfg := pctOpts{depth: 2, maxSteps: 256, seed: 1, maxRuns: 100}
	for _, opt := range opts {
		opt(&cfg)
	}
	algo := &PCT{Depth: cfg.depth, MaxSteps: cfg.maxSteps, Seed: cfg.seed}
	var eopts []ExploreOption
	if cfg.maxRuns > 0 {
		eopts = append(eopts, GlobalMaxRuns(cfg.maxRuns))
	}
	if cfg.observer != nil {
		eopts = append(eopts, WithObserver(cfg.observer))
	}
	return o.ExploreWith(t, setup, algo, eopts...).Failed == 0
}

type RandomOption func(*randomOpts)
type randomOpts struct {
	seed, maxRuns int64
	observer      RunObserver
}

func RandomSeed(s int64) RandomOption  { return func(o *randomOpts) { o.seed = s } }
func RandomMaxRuns(n int) RandomOption { return func(o *randomOpts) { o.maxRuns = int64(n) } }
func RandomWithObserver(fn RunObserver) RandomOption { return func(o *randomOpts) { o.observer = fn } }

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
