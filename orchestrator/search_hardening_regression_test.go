package orchestrator

import (
	"testing"
	"testing/synctest"
)

func TestExploratoryPrefixIdentityAndFinish(t *testing.T) {
	for _, name := range []string{"chess", "dpor"} {
		t.Run(name, func(t *testing.T) {
			var a Algorithm
			if name == "chess" {
				a = &CHESS{Bound: 1}
			} else {
				a = &DPOR{Bound: 1}
			}
			a.BeforeRun()
			dp := DecisionPoint{Kind: Local, Node: "a", Alts: []Alt{{ID: "B1"}, {ID: "B2"}}}
			a.AfterRun(RunResult{Passed: true, Trace: Trace{stepForDecision(dp, 0)}})
			if !a.BeforeRun() {
				t.Fatal("missing branch")
			}
			if err := a.(prefixValidator).ValidatePrefix(); err == nil {
				t.Fatal("short execution accepted")
			}
			swapped := dp
			swapped.Alts = []Alt{{ID: "B2"}, {ID: "B1"}}
			func() {
				defer func() {
					if _, ok := IsReplayDivergence(recover()); !ok {
						t.Error("reordered prefix accepted")
					}
				}()
				a.Decide(swapped)
			}()
			if a.Decide(dp) != 1 {
				t.Fatal("branch identity lost")
			}
			if err := a.(prefixValidator).ValidatePrefix(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSearchFrontierIsBounded(t *testing.T) {
	for _, a := range []Algorithm{&CHESS{Bound: 1, MaxFrontier: 1}, &DPOR{Bound: 1, MaxFrontier: 1}} {
		a.BeforeRun()
		dp := DecisionPoint{Kind: Local, Node: "a", Alts: []Alt{{ID: "B1"}, {ID: "B2"}, {ID: "B3"}}}
		a.AfterRun(RunResult{Passed: true, Trace: Trace{stepForDecision(dp, 0)}})
		if a.(interface{ SearchError() error }).SearchError() == nil {
			t.Fatal("frontier budget not reported")
		}
		if a.BeforeRun() {
			t.Fatal("continued after frontier failure")
		}
	}
}
func TestPCTSingletonDemotionAndSelectNamespace(t *testing.T) {
	p := &PCT{Depth: 2, MaxSteps: 1}
	p.BeforeRun()
	one := DecisionPoint{Kind: Select, Node: "a", Alts: []Alt{{ID: "same-case"}}}
	p.Decide(one)
	if p.priority[pctKey(one, one.Alts[0])] != pctBasePriority {
		t.Fatal("singleton change point lost")
	}
	other := one
	other.Node = "b"
	if pctKey(one, one.Alts[0]) == pctKey(other, other.Alts[0]) {
		t.Fatal("select entities aliased across nodes")
	}
}
func TestSelectOffsetsAreNotGoroutineIDs(t *testing.T) {
	state := synctest.SelectState{Goroutine: 9, Site: 0x123, CaseN: 2, Cases: [16]int32{7, 2}}
	dp := SelectDecisionPoint(state, 3, "a")
	if dp.Kind != Select || dp.Actor != 9 || dp.Alts[1].CaseIndex != 2 || dp.Alts[1].BGID != 0 {
		t.Fatalf("%+v", dp)
	}
	s := stepForDecision(dp, 1)
	if len(s.RunqBGIDs) != 0 || s.ChosenBGID != 0 || s.SelectCases[1] != 2 {
		t.Fatalf("%+v", s)
	}
	rec := UnifiedReplay{Version: 1, SelectControl: true, Trace: Trace{s}, Terminal: OutcomePassed}
	replay, err := NewStrictReplayer(rec)
	if err != nil {
		t.Fatal(err)
	}
	if offset := replay.Decide(dp); dp.Alts[offset].CaseIndex != 2 {
		t.Fatal("case offset interpreted as runtime case")
	}
	dp.Site = "text+999"
	replay, _ = NewStrictReplayer(rec)
	defer func() {
		if _, ok := IsReplayDivergence(recover()); !ok {
			t.Error("changed select site accepted")
		}
	}()
	replay.Decide(dp)
}
func TestDPORFrontierCallbackIsOneShot(t *testing.T) {
	calls := 0
	d := DPOR{Frontier: func(DecisionPoint) (int, bool) { calls++; return 0, false }}
	d.BeforeRun()
	dp := DecisionPoint{Kind: Local, Node: "a", Alts: []Alt{{ID: "B1"}}}
	for i := 0; i < 3; i++ {
		d.Decide(dp)
	}
	if calls != 1 {
		t.Fatalf("calls %d", calls)
	}
}
