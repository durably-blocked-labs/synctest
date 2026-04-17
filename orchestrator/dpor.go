package orchestrator

import (
	"fmt"
	"sort"
	"strings"
)

// DPOR implements stateless dynamic partial-order reduction for the
// orchestrator's resource model.
//
// The dependency relation is intentionally scoped to what the orchestrator can
// observe without runtime memory instrumentation:
//   - global message deliveries conflict when they share a sender or target
//   - local scheduling decisions conflict within the same node
//   - messages with disjoint endpoints are treated as independent
//
// Bound caps the number of non-FIFO choices in a replay prefix. A zero value
// defaults to 2, matching CHESS.
type DPOR struct {
	Bound int

	// GlobalOnly restricts DPOR to cross-node message deliveries. Local
	// decisions stay FIFO and do not consume replay prefix entries.
	GlobalOnly bool

	// PrioritizeEndpoints boosts branches whose alternative touches one of
	// these endpoint/node names. This is a search-order hint only; it does not
	// change DPOR's dependency relation or suppress other branches.
	PrioritizeEndpoints []string

	// ConservativeGlobal treats every pair of global message deliveries as
	// dependent. This is appropriate for protocol-level tests where disjoint
	// endpoints can still race through quorum waits or later causal messages.
	ConservativeGlobal bool

	// PrioritizeRequests boosts request-like messages over acknowledgements and
	// responses at frontier decisions. This is a search-order hint for protocols
	// where request/response races expose bugs.
	PrioritizeRequests bool

	// Frontier, when set, can choose the first decision after a replay prefix.
	// Returning ok=false falls back to DPOR's built-in frontier heuristic.
	Frontier func(DecisionPoint) (choice int, ok bool)

	stack     []dporItem
	current   dporItem
	prefixIdx int
	tree      []RunNode
	count     int
	seen      map[string]struct{}
}

type dporItem struct {
	prefix  Trace
	nonFIFO int
	parent  int
	branch  int
	score   int
}

// BeforeRun prepares the next run. Returns false when exploration is done.
func (d *DPOR) BeforeRun() bool {
	if d.Bound == 0 {
		d.Bound = 2
	}
	if d.count == 0 && d.seen == nil {
		d.seen = make(map[string]struct{})
		d.stack = []dporItem{{prefix: nil, nonFIFO: 0, parent: -1, branch: -1}}
		d.seen[traceKey(nil)] = struct{}{}
	}
	if len(d.stack) == 0 {
		return false
	}
	d.current = d.stack[len(d.stack)-1]
	d.stack = d.stack[:len(d.stack)-1]
	d.prefixIdx = 0
	return true
}

// Decide follows the replay prefix, then uses the frontier scheduler.
func (d *DPOR) Decide(dp DecisionPoint) int {
	if d.GlobalOnly && dp.Kind == Local {
		return 0
	}
	if d.prefixIdx < len(d.current.prefix) {
		p := d.current.prefix[d.prefixIdx]
		if p.Kind != dp.Kind || p.Node != dp.Node {
			panic(replayDivergenceError{Step: dp.Step})
		}
		if int(p.Index) >= dp.N() {
			panic(replayDivergenceError{Step: dp.Step, Index: int(p.Index), QueueSize: dp.N()})
		}
		d.prefixIdx++
		return int(p.Index)
	}
	return d.frontierChoice(dp)
}

// AfterRun records the run and pushes dependent alternatives only.
func (d *DPOR) AfterRun(result RunResult) {
	d.count++

	nodeIdx := len(d.tree)
	d.tree = append(d.tree, RunNode{
		Run:        d.count,
		Parent:     d.current.parent,
		BranchStep: d.current.branch,
		NonFIFO:    countTraceNonFIFO(result.Trace),
		Passed:     result.Passed,
		TraceLen:   len(result.Trace),
	})

	if result.Diverged {
		return
	}

	trace := result.Trace
	if d.GlobalOnly {
		trace = globalOnlyTrace(trace)
	}
	if d.GlobalOnly {
		d.pushDependentAlts(trace, nodeIdx, true)
	} else {
		d.pushDependentAlts(trace, nodeIdx, false)
		d.pushDependentAlts(trace, nodeIdx, true)
	}
}

func (d *DPOR) pushDependentAlts(trace Trace, nodeIdx int, wantGlobal bool) {
	var items []dporItem
	for i := len(d.current.prefix); i < len(trace); i++ {
		step := trace[i]
		if (step.Kind == Global) != wantGlobal {
			continue
		}
		if step.Alternatives <= 1 {
			continue
		}
		for alt := int32(0); alt < step.Alternatives; alt++ {
			if alt == step.Index {
				continue
			}
			if !d.traceAltDependent(trace, i, int(alt)) {
				continue
			}
			newNonFIFO := countTraceNonFIFO(trace[:i])
			if alt != 0 {
				newNonFIFO++
			}
			if newNonFIFO > d.Bound {
				continue
			}

			newPrefix := make(Trace, i+1)
			copy(newPrefix, trace[:i])
			newPrefix[i] = NewStep{
				Kind:         step.Kind,
				Node:         step.Node,
				Index:        alt,
				Alternatives: step.Alternatives,
				ChosenID:     stepAltID(step, int(alt)),
				Resource:     stepAltResource(step, int(alt)),
				AltIDs:       cloneStrings(step.AltIDs),
				AltResources: cloneStrings(step.AltResources),
			}

			key := traceKey(newPrefix)
			if _, ok := d.seen[key]; ok {
				continue
			}
			d.seen[key] = struct{}{}
			items = append(items, dporItem{
				prefix:  newPrefix,
				nonFIFO: newNonFIFO,
				parent:  nodeIdx,
				branch:  i,
				score:   d.branchScore(trace, i, int(alt)),
			})
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].score < items[j].score
	})
	d.stack = append(d.stack, items...)
}

func (d *DPOR) branchScore(trace Trace, stepIdx int, alt int) int {
	score := -stepIdx
	resource := stepAltResource(trace[stepIdx], alt)
	for _, endpoint := range d.PrioritizeEndpoints {
		if resourceTouchesEndpoint(resource, endpoint) {
			score += 1_000_000
		}
	}
	return score
}

func (d *DPOR) frontierChoice(dp DecisionPoint) int {
	if d.Frontier != nil {
		if choice, ok := d.Frontier(dp); ok {
			return choice
		}
	}
	if len(d.PrioritizeEndpoints) == 0 || dp.Kind != Global {
		return dp.N() - 1
	}
	chosen := 0
	chosenScore := d.altEndpointScore(dp.Alts[0])
	for i := 1; i < dp.N(); i++ {
		score := d.altEndpointScore(dp.Alts[i])
		if score > chosenScore {
			chosen = i
			chosenScore = score
		}
	}
	return chosen
}

func (d *DPOR) altEndpointScore(alt Alt) int {
	score := 0
	if d.PrioritizeRequests && !isResponseMessage(alt.MsgType) {
		score += (len(d.PrioritizeEndpoints) + 1) * 10
	}
	for i, endpoint := range d.PrioritizeEndpoints {
		weight := len(d.PrioritizeEndpoints) - i
		if alt.From == endpoint {
			score += weight
		}
		if alt.To == endpoint {
			score += weight
		}
	}
	return score
}

func isResponseMessage(msgType string) bool {
	return strings.HasSuffix(msgType, "Ack") || strings.HasSuffix(msgType, "Resp")
}

func (d *DPOR) traceAltDependent(trace Trace, stepIdx int, alt int) bool {
	step := trace[stepIdx]
	altResource := stepAltResource(step, alt)
	if d.stepAltDependent(step, alt) {
		return true
	}
	altID := stepAltID(step, alt)
	for i := stepIdx + 1; i < len(trace); i++ {
		if altID != "" && trace[i].ChosenID == altID {
			return false
		}
		if d.resourcesDependent(altResource, stepResource(trace[i])) {
			return true
		}
	}
	return false
}

func (d *DPOR) stepAltDependent(step NewStep, alt int) bool {
	return d.resourcesDependent(stepResource(step), stepAltResource(step, alt))
}

func (d *DPOR) resourcesDependent(a, b string) bool {
	if d.ConservativeGlobal {
		_, _, aEndpoint := splitEndpointResource(a)
		_, _, bEndpoint := splitEndpointResource(b)
		if aEndpoint && bEndpoint {
			return true
		}
	}
	return resourcesDependent(a, b)
}

func resourcesDependent(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	aFrom, aTo, aEndpoint := splitEndpointResource(a)
	bFrom, bTo, bEndpoint := splitEndpointResource(b)
	if aEndpoint && !bEndpoint {
		return aFrom == b || aTo == b
	}
	if !aEndpoint && bEndpoint {
		return a == bFrom || a == bTo
	}
	if !aEndpoint || !bEndpoint {
		return false
	}
	return aFrom == bFrom || aFrom == bTo || aTo == bFrom || aTo == bTo
}

func resourceTouchesEndpoint(resource, endpoint string) bool {
	if resource == "" || endpoint == "" {
		return false
	}
	from, to, endpointResource := splitEndpointResource(resource)
	if !endpointResource {
		return resource == endpoint
	}
	return from == endpoint || to == endpoint
}

func splitEndpointResource(resource string) (string, string, bool) {
	left, right, ok := strings.Cut(resource, "|")
	if !ok {
		return "", "", false
	}
	return left, right, true
}

func stepResource(step NewStep) string {
	if step.Resource != "" {
		return step.Resource
	}
	if step.Kind == Local {
		return step.Node
	}
	return step.To
}

func stepAltResource(step NewStep, alt int) string {
	if alt >= 0 && alt < len(step.AltResources) {
		return step.AltResources[alt]
	}
	if step.Kind == Local {
		return step.Node
	}
	if alt == int(step.Index) {
		return stepResource(step)
	}
	return ""
}

func stepAltID(step NewStep, alt int) string {
	if alt >= 0 && alt < len(step.AltIDs) {
		return step.AltIDs[alt]
	}
	return ""
}

func globalOnlyTrace(trace Trace) Trace {
	out := make(Trace, 0, len(trace))
	for _, step := range trace {
		if step.Kind == Global {
			out = append(out, step)
		}
	}
	return out
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func countTraceNonFIFO(trace Trace) int {
	n := 0
	for _, step := range trace {
		if step.Index != 0 {
			n++
		}
	}
	return n
}

func traceKey(trace Trace) string {
	var b strings.Builder
	for i, step := range trace {
		if i > 0 {
			b.WriteByte('|')
		}
		fmt.Fprintf(&b, "%d:%s:%d", step.Kind, step.Node, step.Index)
		if step.ChosenID != "" {
			fmt.Fprintf(&b, ":%s", step.ChosenID)
		}
	}
	return b.String()
}

// Tree returns the exploration tree for benchmarking export.
func (d *DPOR) Tree() []RunNode {
	return d.tree
}

// Count returns the number of runs completed so far.
func (d *DPOR) Count() int {
	return d.count
}
