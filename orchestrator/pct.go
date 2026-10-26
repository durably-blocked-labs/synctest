package orchestrator

import (
	"encoding/json"
	"math/rand"
	"sort"
	"testing"
)

const pctBasePriority = 1 << 30

// PCTPolicyVersion must be included in campaign policy provenance. Unlike v1,
// singleton decisions initialize priorities and consume sampled demotions;
// subsequent seeded schedules are not promised byte-identical compatibility.
const PCTPolicyVersion = "pct-all-decisions/v2"

// ─── PCT Algorithm ───

// PCT implements combined global + local PCT scheduling.
//
// Every schedulable entity (goroutine or message class) gets a random
// priority. At each decision point — global delivery OR local goroutine
// scheduling — the entity with the lowest priority number wins.
// At d-1 randomly sampled decision steps (including singleton steps), the
// winning entity is demoted.
//
// Global entities are identified by Alt.ID (message class, e.g. "msg:A→B(Request)").
// Local entities are identified by node and Alt.ID (bubble goroutine ID, e.g. "B5").
type PCT struct {
	Depth    int   // priority changes = Depth-1 (default 2)
	MaxSteps int   // step range for sampling change points (default 256)
	Seed     int64 // base seed, incremented per run

	rng          *rand.Rand
	priority     map[pctPriorityKey]int
	nextLow      int
	changePoints map[int]struct{}
	step         int // current step within the run (tracks G+L)
	count        int
}

type pctCheckpoint struct {
	checkpointMetadata
	Count int `json:"count"`
}

var _ CheckpointableAlgorithm = (*PCT)(nil)

func (p *PCT) MarshalCheckpoint() ([]byte, error) {
	return json.Marshal(pctCheckpoint{
		checkpointMetadata: checkpointMetadata{Version: checkpointVersion, Algorithm: "pct"},
		Count:              p.count,
	})
}

func (p *PCT) UnmarshalCheckpoint(data []byte) error {
	var checkpoint pctCheckpoint
	if err := validateCheckpoint(data, "pct", &checkpoint); err != nil {
		return err
	}
	p.count = checkpoint.Count
	p.rng = nil
	p.priority = nil
	p.nextLow = 0
	p.changePoints = nil
	p.step = 0
	return nil
}

type pctPriorityKey struct {
	kind DecisionKind
	node string
	id   string
}

func pctKey(dp DecisionPoint, alt Alt) pctPriorityKey {
	key := pctPriorityKey{kind: dp.Kind, id: alt.ID}
	if dp.Kind != Global {
		key.node = dp.Node
	}
	return key
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
	p.priority = make(map[pctPriorityKey]int)
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

// Decide picks the alternative with the lowest priority at each scheduling point.
func (p *PCT) Decide(dp DecisionPoint) int {
	if dp.N() == 0 {
		panic("PCT: empty decision")
	}
	chosen := 0
	chosenPri := p.priorityOf(pctKey(dp, dp.Alts[0]))
	for i := 1; i < dp.N(); i++ {
		pri := p.priorityOf(pctKey(dp, dp.Alts[i]))
		if pri < chosenPri {
			chosen = i
			chosenPri = pri
		}
	}

	// At change points, demote the winner.
	if _, ok := p.changePoints[p.step]; ok {
		p.priority[pctKey(dp, dp.Alts[chosen])] = p.nextLow
		p.nextLow++
	}

	p.step++
	return chosen
}

func (p *PCT) AfterRun(result RunResult) {
	p.count++
}

func (p *PCT) priorityOf(key pctPriorityKey) int {
	if pri, ok := p.priority[key]; ok {
		return pri
	}
	pri := p.rng.Intn(pctBasePriority)
	p.priority[key] = pri
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

// ─── Convenience ───

type PCTOption func(*pctOpts)
type pctOpts struct {
	depth, maxSteps, maxRuns int
	seed                     int64
	observer                 RunObserver
}

func PCTDepth(d int) PCTOption                 { return func(o *pctOpts) { o.depth = d } }
func PCTMaxSteps(n int) PCTOption              { return func(o *pctOpts) { o.maxSteps = n } }
func PCTSeed(s int64) PCTOption                { return func(o *pctOpts) { o.seed = s } }
func PCTMaxRuns(n int) PCTOption               { return func(o *pctOpts) { o.maxRuns = n } }
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
	return o.ExploreWith(t, setup, algo, eopts...).Successful()
}
