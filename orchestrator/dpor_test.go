package orchestrator

import "testing"

func TestDPORPrunesIndependentGlobalAlternatives(t *testing.T) {
	algo := &DPOR{Bound: 4}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	algo.AfterRun(RunResult{
		Passed: true,
		Trace: Trace{
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:A->R1(Put)",
				Resource:     "R1",
				AltIDs:       []string{"msg:A->R1(Put)", "msg:B->R2(Put)"},
				AltResources: []string{"R1", "R2"},
			},
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 1,
				ChosenID:     "msg:B->R2(Put)",
				Resource:     "R2",
				AltIDs:       []string{"msg:B->R2(Put)"},
				AltResources: []string{"R2"},
			},
		},
	})

	if algo.BeforeRun() {
		t.Fatal("independent messages to different target nodes should not create a DPOR branch")
	}
}

func TestDPORConservativeGlobalBranchesIndependentEndpoints(t *testing.T) {
	algo := &DPOR{Bound: 4, ConservativeGlobal: true}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	algo.AfterRun(RunResult{
		Passed: true,
		Trace: Trace{
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:A->R1(Put)",
				Resource:     "A|R1",
				AltIDs:       []string{"msg:A->R1(Put)", "msg:B->R2(Put)"},
				AltResources: []string{"A|R1", "B|R2"},
			},
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 1,
				ChosenID:     "msg:B->R2(Put)",
				Resource:     "B|R2",
				AltIDs:       []string{"msg:B->R2(Put)"},
				AltResources: []string{"B|R2"},
			},
		},
	})

	if !algo.BeforeRun() {
		t.Fatal("conservative global mode should branch even for disjoint endpoints")
	}
}

func TestDPORBranchesDependentGlobalAlternatives(t *testing.T) {
	algo := &DPOR{Bound: 4}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	algo.AfterRun(RunResult{
		Passed: true,
		Trace: Trace{
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:A->R1(Put)",
				Resource:     "R1",
				AltIDs:       []string{"msg:A->R1(Put)", "msg:B->R1(Repair)"},
				AltResources: []string{"R1", "R1"},
			},
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 1,
				ChosenID:     "msg:B->R1(Repair)",
				Resource:     "R1",
				AltIDs:       []string{"msg:B->R1(Repair)"},
				AltResources: []string{"R1"},
			},
		},
	})

	if !algo.BeforeRun() {
		t.Fatal("dependent messages to the same target node should create a DPOR branch")
	}
	dp := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{
			{ID: "msg:A->R1(Put)", To: "R1"},
			{ID: "msg:B->R1(Repair)", To: "R1"},
		},
	}
	if got := algo.Decide(dp); got != 1 {
		t.Fatalf("branch decision = %d, want 1", got)
	}
}

func TestDPORBranchesSameSenderGlobalAlternatives(t *testing.T) {
	algo := &DPOR{Bound: 4}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	algo.AfterRun(RunResult{
		Passed: true,
		Trace: Trace{
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:C1->R1(Put)",
				Resource:     "C1|R1",
				AltIDs:       []string{"msg:C1->R1(Put)", "msg:C1->Reader(Control)"},
				AltResources: []string{"C1|R1", "C1|Reader"},
			},
		},
	})

	if !algo.BeforeRun() {
		t.Fatal("messages from the same sender should create a conservative DPOR branch")
	}
	dp := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{
			{ID: "msg:C1->R1(Put)", From: "C1", To: "R1"},
			{ID: "msg:C1->Reader(Control)", From: "C1", To: "Reader"},
		},
	}
	if got := algo.Decide(dp); got != 1 {
		t.Fatalf("branch decision = %d, want 1", got)
	}
}

func TestDPORBranchesLocalAlternativesWithinSameNode(t *testing.T) {
	algo := &DPOR{Bound: 4}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	algo.AfterRun(RunResult{
		Passed: true,
		Trace: Trace{
			{
				Kind:         Local,
				Node:         "R1",
				Index:        0,
				Alternatives: 2,
				ChosenID:     "B10",
				Resource:     "R1",
				AltIDs:       []string{"B10", "B11"},
				AltResources: []string{"R1", "R1"},
			},
		},
	})

	if !algo.BeforeRun() {
		t.Fatal("local alternatives in the same node should create a DPOR branch")
	}
	dp := DecisionPoint{
		Kind: Local,
		Step: 0,
		Node: "R1",
		Alts: []Alt{
			{ID: "B10", BGID: 10},
			{ID: "B11", BGID: 11},
		},
	}
	if got := algo.Decide(dp); got != 1 {
		t.Fatalf("branch decision = %d, want 1", got)
	}
}

func TestDPORTraceKeyDistinguishesLocalGoroutineChoice(t *testing.T) {
	left := Trace{{
		Kind:     Local,
		Node:     "R1",
		Index:    1,
		ChosenID: "B10",
	}}
	right := Trace{{
		Kind:     Local,
		Node:     "R1",
		Index:    1,
		ChosenID: "B11",
	}}

	if traceKey(left) == traceKey(right) {
		t.Fatal("local choices with the same index but different goroutines should not collapse to the same DPOR key")
	}
}

func TestDPORTraceKeyDistinguishesGlobalMessageChoice(t *testing.T) {
	left := Trace{{
		Kind:     Global,
		Index:    1,
		ChosenID: "msg:C2->R1(Put)",
	}}
	right := Trace{{
		Kind:     Global,
		Index:    1,
		ChosenID: "msg:Reader->R1(Get)",
	}}

	if traceKey(left) == traceKey(right) {
		t.Fatal("global choices with the same index but different messages should not collapse to the same DPOR key")
	}
}

func TestDPORPrioritizesEndpointBranches(t *testing.T) {
	algo := &DPOR{Bound: 4, PrioritizeEndpoints: []string{"R1"}}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	algo.AfterRun(RunResult{
		Passed: true,
		Trace: Trace{
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:A->R2(Put)",
				Resource:     "A|R2",
				AltIDs:       []string{"msg:A->R2(Put)", "msg:B->R2(Put)"},
				AltResources: []string{"A|R2", "B|R2"},
			},
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:C->R1(Put)",
				Resource:     "C|R1",
				AltIDs:       []string{"msg:C->R1(Put)", "msg:D->R1(Put)"},
				AltResources: []string{"C|R1", "D|R1"},
			},
		},
	})

	if !algo.BeforeRun() {
		t.Fatal("expected prioritized branch")
	}
	first := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{
			{ID: "msg:A->R2(Put)", From: "A", To: "R2"},
			{ID: "msg:B->R2(Put)", From: "B", To: "R2"},
		},
	}
	if got := algo.Decide(first); got != 0 {
		t.Fatalf("first prefix decision = %d, want 0", got)
	}
	second := DecisionPoint{
		Kind: Global,
		Step: 1,
		Alts: []Alt{
			{ID: "msg:C->R1(Put)", From: "C", To: "R1"},
			{ID: "msg:D->R1(Put)", From: "D", To: "R1"},
		},
	}
	if got := algo.Decide(second); got != 1 {
		t.Fatalf("prioritized endpoint branch decision = %d, want 1", got)
	}
}

func TestDPORPrioritizesEarlierNeutralBranches(t *testing.T) {
	algo := &DPOR{Bound: 4}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	algo.AfterRun(RunResult{
		Passed: true,
		Trace: Trace{
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:A->R1(Put)",
				Resource:     "A|R1",
				AltIDs:       []string{"msg:A->R1(Put)", "msg:B->R1(Put)"},
				AltResources: []string{"A|R1", "B|R1"},
			},
			{
				Kind:         Global,
				Index:        0,
				Alternatives: 2,
				ChosenID:     "msg:C->R2(Put)",
				Resource:     "C|R2",
				AltIDs:       []string{"msg:C->R2(Put)", "msg:D->R2(Put)"},
				AltResources: []string{"C|R2", "D|R2"},
			},
		},
	})

	if !algo.BeforeRun() {
		t.Fatal("expected branch")
	}
	first := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{
			{ID: "msg:A->R1(Put)", From: "A", To: "R1"},
			{ID: "msg:B->R1(Put)", From: "B", To: "R1"},
		},
	}
	if got := algo.Decide(first); got != 1 {
		t.Fatalf("first neutral branch decision = %d, want 1", got)
	}
}

func TestDPORPrioritizesFrontierDecisionByEndpointOrder(t *testing.T) {
	algo := &DPOR{Bound: 4, PrioritizeEndpoints: []string{"Reader", "R1"}}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	dp := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{
			{ID: "msg:C2->R1(Put)", From: "C2", To: "R1"},
			{ID: "msg:Reader->R2(Get)", From: "Reader", To: "R2"},
			{ID: "msg:Reader->R1(Get)", From: "Reader", To: "R1"},
		},
	}
	if got := algo.Decide(dp); got != 2 {
		t.Fatalf("frontier decision = %d, want 2", got)
	}
}

func TestDPORNeutralFrontierChoosesNewestAlternative(t *testing.T) {
	algo := &DPOR{Bound: 4}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	dp := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{
			{ID: "msg:A->R1(Put)", From: "A", To: "R1"},
			{ID: "msg:B->R2(Put)", From: "B", To: "R2"},
			{ID: "msg:C->R3(Put)", From: "C", To: "R3"},
		},
	}
	if got := algo.Decide(dp); got != 2 {
		t.Fatalf("neutral frontier decision = %d, want 2", got)
	}
}

func TestDPORPrioritizesRequestsOverResponsesAtFrontier(t *testing.T) {
	algo := &DPOR{
		Bound:               4,
		PrioritizeEndpoints: []string{"Reader", "R1"},
		PrioritizeRequests:  true,
	}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	dp := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{
			{ID: "msg:R1->Reader(RepairAck)", From: "R1", To: "Reader", MsgType: "RepairAck"},
			{ID: "msg:C2->R1(Put)", From: "C2", To: "R1", MsgType: "Put"},
		},
	}
	if got := algo.Decide(dp); got != 1 {
		t.Fatalf("frontier decision = %d, want 1", got)
	}
}

func TestDPORUsesCustomFrontierChoice(t *testing.T) {
	algo := &DPOR{
		Bound: 4,
		Frontier: func(dp DecisionPoint) (int, bool) {
			return dp.N() - 1, true
		},
	}
	if !algo.BeforeRun() {
		t.Fatal("first run was not scheduled")
	}

	dp := DecisionPoint{
		Kind: Global,
		Step: 0,
		Alts: []Alt{{ID: "a"}, {ID: "b"}, {ID: "c"}},
	}
	if got := algo.Decide(dp); got != 2 {
		t.Fatalf("frontier decision = %d, want 2", got)
	}
}
