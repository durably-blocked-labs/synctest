package orchestrator

import (
	"fmt"
	"reflect"
)

// ReplayVersion records schedule, select, and message-occurrence semantics.
const ReplayVersion = 1

// UnifiedReplay is exact same-binary replay, not an exploratory prefix.
// Inputs, reset policy and binary identity must also match the campaign manifest.
type UnifiedReplay struct {
	SelectControl bool
	Version       int
	Trace         Trace
	Terminal      Outcome
	TerminalCause string
	GlobalTrace   []GlobalStep
}

type StrictReplayer struct {
	record UnifiedReplay
	next   int
}

func NewStrictReplayer(rec UnifiedReplay) (*StrictReplayer, error) {
	if rec.Version != ReplayVersion {
		return nil, fmt.Errorf("unsupported replay version %d", rec.Version)
	}
	if rec.Terminal == "" {
		return nil, fmt.Errorf("missing replay terminal outcome")
	}
	if len(rec.Trace) > 512 {
		return nil, fmt.Errorf("replay exceeds supported decision capacity")
	}
	for i, s := range rec.Trace {
		if s.Kind > Select || s.Alternatives < 1 || s.Index < 0 || s.Index >= s.Alternatives || len(s.AltIDs) != int(s.Alternatives) || s.ChosenID != s.AltIDs[s.Index] {
			return nil, fmt.Errorf("invalid replay decision %d", i)
		}
		if s.Kind == Global && (len(s.AltOccurrences) != int(s.Alternatives) || s.ChosenOccurrence == "" || s.ChosenOccurrence != s.AltOccurrences[s.Index]) {
			return nil, fmt.Errorf("missing occurrence at decision %d", i)
		}
		if s.Kind == Global {
			seen := map[string]bool{}
			for _, id := range s.AltOccurrences {
				if id == "" || seen[id] {
					return nil, fmt.Errorf("missing/duplicate occurrence at decision %d", i)
				}
				seen[id] = true
			}
		}
		if s.Kind == Select && (!rec.SelectControl || s.Site == "" || len(s.SelectCases) != int(s.Alternatives)) {
			return nil, fmt.Errorf("invalid select identity at decision %d", i)
		}
	}
	return &StrictReplayer{record: rec}, nil
}

func (r *StrictReplayer) Decide(dp DecisionPoint) int {
	if r.next >= len(r.record.Trace) {
		panic(replayDivergenceError{Step: dp.Step, Detail: "extra execution decision"})
	}
	s := r.record.Trace[r.next]
	if err := matchStep(s, dp, true); err != nil {
		panic(replayDivergenceError{Step: dp.Step, Detail: err.Error()})
	}
	r.next++
	return int(s.Index)
}

func (r *StrictReplayer) Finish(rr RunResult) error {
	if r.next != len(r.record.Trace) {
		return replayDivergenceError{Step: r.next, Detail: "unused replay suffix"}
	}
	if rr.Outcome != r.record.Terminal {
		return replayDivergenceError{Step: r.next, Detail: fmt.Sprintf("terminal outcome %q, expected %q", rr.Outcome, r.record.Terminal)}
	}
	if causeDiagnostic(rr.Cause) != r.record.TerminalCause {
		return replayDivergenceError{Step: r.next, Detail: "terminal cause changed"}
	}
	if !reflect.DeepEqual(rr.GlobalTrace, r.record.GlobalTrace) {
		return replayDivergenceError{Step: r.next, Detail: "global delivery/time result changed"}
	}
	return nil
}

// ValidatePrefix is optional on exploratory algorithms. A shorter execution is
// divergence, not a successful execution of the requested branch.
type prefixValidator interface{ ValidatePrefix() error }

func (s *CHESS) ValidatePrefix() error {
	if s.prefixIdx != len(s.current.prefix) {
		return replayDivergenceError{Step: s.prefixIdx, Detail: "unused exploratory prefix"}
	}
	return nil
}
func (d *DPOR) ValidatePrefix() error {
	if d.prefixIdx != len(d.current.prefix) {
		return replayDivergenceError{Step: d.prefixIdx, Detail: "unused exploratory prefix"}
	}
	return nil
}

func matchStep(s NewStep, dp DecisionPoint, strict bool) error {
	if s.Kind != dp.Kind || s.Node != dp.Node || s.Index < 0 || int(s.Index) >= dp.N() {
		return fmt.Errorf("decision kind/node/index changed")
	}
	ids := make([]string, dp.N())
	occurrences := make([]string, dp.N())
	cases := make([]int32, dp.N())
	for i, a := range dp.Alts {
		ids[i] = a.ID
		occurrences[i] = a.Occurrence
		cases[i] = a.CaseIndex
	}
	if (strict || s.Alternatives != 0) && int(s.Alternatives) != dp.N() {
		return fmt.Errorf("alternative cardinality changed")
	}
	if (strict || len(s.AltIDs) > 0) && !reflect.DeepEqual(s.AltIDs, ids) {
		return fmt.Errorf("alternative order/identity changed")
	}
	if s.ChosenID != "" && s.ChosenID != ids[s.Index] {
		return fmt.Errorf("chosen identity changed")
	}
	if dp.Kind == Global && (strict || len(s.AltOccurrences) > 0) && !reflect.DeepEqual(s.AltOccurrences, occurrences) {
		return fmt.Errorf("message occurrences changed")
	}
	if dp.Kind == Select && (s.Site != dp.Site || s.Actor != dp.Actor || !reflect.DeepEqual(s.SelectCases, cases)) {
		return fmt.Errorf("select site/actor/cases changed")
	}
	return nil
}

func branchStep(s NewStep, alt int32) NewStep {
	s.Index = alt
	if int(alt) < len(s.AltIDs) {
		s.ChosenID = s.AltIDs[alt]
	}
	if int(alt) < len(s.AltResources) {
		s.Resource = s.AltResources[alt]
	}
	if int(alt) < len(s.AltOccurrences) {
		s.ChosenOccurrence = s.AltOccurrences[alt]
	}
	return s
}

// IsReplayDivergence distinguishes expected prefix rejection from defects.
func IsReplayDivergence(cause any) (int, bool) {
	switch d := cause.(type) {
	case replayDivergenceError:
		return d.Step, true
	case *replayDivergenceError:
		return d.Step, true
	}
	return -1, false
}

func CheckDecision(dp DecisionPoint, index int) {
	if index < 0 || index >= dp.N() {
		panic(fmt.Errorf("algorithm returned index %d for %d alternatives at step %d", index, dp.N(), dp.Step))
	}
}

func stepForDecision(dp DecisionPoint, idx int) NewStep {
	CheckDecision(dp, idx)
	s := NewStep{Kind: dp.Kind, Node: dp.Node, Index: int32(idx), Alternatives: int32(dp.N()), ChosenID: dp.Alts[idx].ID, ChosenOccurrence: dp.Alts[idx].Occurrence, Site: dp.Site, Actor: dp.Actor}
	for _, a := range dp.Alts {
		s.AltIDs = append(s.AltIDs, a.ID)
		s.AltOccurrences = append(s.AltOccurrences, a.Occurrence)
		if dp.Kind == Select {
			s.SelectCases = append(s.SelectCases, a.CaseIndex)
		}
	}
	s.AltIDs, s.AltResources = altIDsAndResources(dp)
	if dp.Kind == Global {
		a := dp.Alts[idx]
		s.From = a.From
		s.To = a.To
		s.MsgType = a.MsgType
		s.Resource = endpointResource(a.From, a.To)
	} else {
		s.Resource = dp.Node
	}
	if dp.Kind == Local {
		s.ChosenBGID = dp.Alts[idx].BGID
		for _, a := range dp.Alts {
			s.RunqBGIDs = append(s.RunqBGIDs, a.BGID)
		}
	}
	return s
}

// RecordDecision copies the ephemeral alternative identities for exact replay.
func RecordDecision(dp DecisionPoint, idx int) NewStep { return stepForDecision(dp, idx) }

func causeDiagnostic(c any) string {
	if c == nil {
		return ""
	}
	return fmt.Sprint(c)
}
