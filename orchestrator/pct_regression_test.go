package orchestrator

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestPCTLocalDemotionIsNodeScoped(t *testing.T) {
	p := &PCT{Depth: 1, Seed: 1}
	p.BeforeRun()
	a := DecisionPoint{Kind: Local, Node: "A", Alts: []Alt{{ID: "B5", BGID: 5}, {ID: "B6", BGID: 6}}}
	b := a
	b.Node = "B"
	p.Decide(a)
	before := p.Decide(b)
	p.changePoints[p.step] = struct{}{}
	p.Decide(a) // Demote A's winner, not B's equal bubble-local ID.
	if after := p.Decide(b); after != before {
		t.Fatalf("node A's demotion changed node B's winner from %d to %d", before, after)
	}
}

func TestPCTLocalDemotionDoesNotAliasGlobalClass(t *testing.T) {
	p := &PCT{Depth: 1, Seed: 1}
	p.BeforeRun()
	local := DecisionPoint{Kind: Local, Node: "A", Alts: []Alt{{ID: "same"}, {ID: "other"}}}
	global := local
	global.Kind = Global
	global.Node = ""
	p.Decide(local)
	before := p.Decide(global)
	p.changePoints[p.step] = struct{}{}
	p.Decide(local)
	if after := p.Decide(global); after != before {
		t.Fatalf("local demotion changed global winner from %d to %d", before, after)
	}
}

func TestPCTPreservesGlobalClassPolicyAndSeedProgression(t *testing.T) {
	p := &PCT{Depth: 1, Seed: 19}
	for run := range 2 {
		p.BeforeRun()
		rng := rand.New(rand.NewSource(19 + int64(run)))
		priorities := []int{rng.Intn(pctBasePriority), rng.Intn(pctBasePriority)}
		want := 0
		if priorities[1] < priorities[0] {
			want = 1
		}
		dp := DecisionPoint{Kind: Global, Alts: []Alt{{ID: "msg:A→B(Request)"}, {ID: "msg:C→D(Request)"}}}
		if got := p.Decide(dp); got != want {
			t.Fatalf("run %d global choice=%d, want %d", run, got, want)
		}
		p.changePoints[p.step] = struct{}{}
		p.Decide(dp)
		// Another occurrence of the same classes shares the demotion.
		if got := p.Decide(dp); got != 1-want {
			t.Fatalf("run %d repeated global classes choice=%d, want %d", run, got, 1-want)
		}
		p.AfterRun(RunResult{Passed: true})
	}
	if p.Count() != 2 {
		t.Fatalf("completed run count=%d, want 2", p.Count())
	}
}

func TestPCTSingletonAllDecisionChangePointPolicy(t *testing.T) {
	p := &PCT{Depth: 2, MaxSteps: 8, Seed: 7}
	p.BeforeRun()
	rng := rand.New(rand.NewSource(7))
	permutation := rng.Perm(8)
	if !reflect.DeepEqual(p.SortedChangePoints(), []int{permutation[0]}) {
		t.Fatalf("sampled change points=%v, want [%d]", p.SortedChangePoints(), permutation[0])
	}
	p.changePoints = map[int]struct{}{0: {}}
	if got := p.Decide(DecisionPoint{Kind: Local, Node: "A", Alts: []Alt{{ID: "B5"}}}); got != 0 {
		t.Fatalf("singleton choice=%d, want 0", got)
	}
	key := pctPriorityKey{kind: Local, node: "A", id: "B5"}
	if p.step != 1 || len(p.priority) != 1 || p.priority[key] != pctBasePriority || p.nextLow != pctBasePriority+1 {
		t.Fatalf("singleton was not demoted: step=%d priorities=%v nextLow=%d", p.step, p.priority, p.nextLow)
	}
	// All decision steps, including singleton steps, are the sampling domain.
	if got := p.Decide(DecisionPoint{Kind: Local, Node: "A", Alts: []Alt{{ID: "B5"}, {ID: "B6"}}}); got != 1 {
		t.Fatalf("demoted singleton won contested decision: %d", got)
	}
}
