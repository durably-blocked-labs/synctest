package orchestrator

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type dporCheckpointItem struct {
	Prefix  Trace `json:"prefix"`
	NonFIFO int   `json:"non_fifo"`
	Parent  int   `json:"parent"`
	Branch  int   `json:"branch"`
	Score   int   `json:"score"`
}

type dporCheckpoint struct {
	checkpointMetadata
	Stack []dporCheckpointItem `json:"stack"`
	Seen  map[string]struct{}  `json:"seen"`
	Count int                  `json:"count"`
	Tree  []RunNode            `json:"tree"`
}

var _ CheckpointableAlgorithm = (*DPOR)(nil)

func (d *DPOR) MarshalCheckpoint() ([]byte, error) {
	stack := make([]dporCheckpointItem, len(d.stack))
	for i, item := range d.stack {
		stack[i] = dporCheckpointItem{
			Prefix:  item.prefix,
			NonFIFO: item.nonFIFO,
			Parent:  item.parent,
			Branch:  item.branch,
			Score:   item.score,
		}
	}
	return json.Marshal(dporCheckpoint{
		checkpointMetadata: checkpointMetadata{Version: checkpointVersion, Algorithm: "dpor"},
		Stack:              stack,
		Seen:               d.seen,
		Count:              d.count,
		Tree:               d.tree,
	})
}

func (d *DPOR) UnmarshalCheckpoint(data []byte) error {
	var checkpoint dporCheckpoint
	if err := validateCheckpoint(data, "dpor", &checkpoint); err != nil {
		return err
	}
	candidate := *d
	candidate.MaxFrontier = checkpointCap(d.MaxFrontier, 4096)
	candidate.MaxFrontierSteps = checkpointCap(d.MaxFrontierSteps, 1<<20)
	candidate.MaxTree = checkpointCap(d.MaxTree, 10000)
	candidate.MaxSeen = checkpointCap(d.MaxSeen, 10000)
	if candidate.Bound == 0 {
		candidate.Bound = 2
	}
	if candidate.Bound < 0 || len(checkpoint.Stack) > candidate.MaxFrontier || len(checkpoint.Seen) > candidate.MaxSeen {
		return fmt.Errorf("checkpoint exceeds DPOR bound/frontier/seen limit")
	}
	if err := validateCheckpointTree(checkpoint.Count, checkpoint.Tree, candidate.MaxTree); err != nil {
		return err
	}
	if checkpoint.Count > 0 && checkpoint.Seen == nil {
		return fmt.Errorf("checkpoint is missing DPOR seen state")
	}
	if checkpoint.Count == 0 && len(checkpoint.Stack) == 0 && checkpoint.Seen != nil {
		return fmt.Errorf("checkpoint contains an unfinished initial DPOR run")
	}
	steps := 0
	pending := make(map[string]bool, len(checkpoint.Stack))
	stack := make([]dporItem, len(checkpoint.Stack))
	for i, item := range checkpoint.Stack {
		if len(item.Prefix) > candidate.MaxFrontierSteps-steps {
			return fmt.Errorf("checkpoint exceeds DPOR frontier-step limit")
		}
		if err := validateCheckpointPrefix(item.Prefix, item.NonFIFO, item.Parent, item.Branch, candidate.Bound, checkpoint.Tree); err != nil {
			return err
		}
		key := traceKey(item.Prefix)
		if _, ok := checkpoint.Seen[key]; !ok || pending[key] {
			return fmt.Errorf("checkpoint DPOR frontier/seen state disagrees")
		}
		pending[key] = true
		steps += len(item.Prefix)
		stack[i] = dporItem{
			prefix:  item.Prefix,
			nonFIFO: item.NonFIFO,
			parent:  item.Parent,
			branch:  item.Branch,
			score:   item.Score,
		}
	}
	candidate.stack = stack
	candidate.seen = checkpoint.Seen
	candidate.count = checkpoint.Count
	candidate.tree = checkpoint.Tree
	candidate.current = dporItem{}
	candidate.prefixIdx = 0
	candidate.frontierCalled = false
	candidate.frontierSteps = steps
	candidate.budgetErr = nil
	*d = candidate
	return nil
}

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
	// Explicit retained search-state safety caps. Zero uses bounded defaults.
	MaxFrontier      int
	MaxFrontierSteps int
	MaxTree          int
	MaxSeen          int
	frontierSteps    int
	budgetErr        error

	Bound int

	// PrioritizeEndpoints boosts branches whose alternative touches one of
	// these endpoint/node names. This is a search-order hint only; it does not
	// change DPOR's dependency relation or suppress other branches.
	PrioritizeEndpoints []string

	// PrioritizeRequests boosts request-like messages over acknowledgements and
	// responses at frontier decisions. This is a search-order hint for protocols
	// where request/response races expose bugs.
	PrioritizeRequests bool

	// Frontier, when set, can choose the first decision after a replay prefix.
	// Returning ok=false falls back to DPOR's built-in frontier heuristic.
	Frontier func(DecisionPoint) (choice int, ok bool)

	stack          []dporItem
	current        dporItem
	prefixIdx      int
	frontierCalled bool
	tree           []RunNode
	count          int
	seen           map[string]struct{}
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
	if d.budgetErr != nil {
		return false
	}
	if d.MaxFrontier <= 0 {
		d.MaxFrontier = 4096
	}
	if d.MaxFrontierSteps <= 0 {
		d.MaxFrontierSteps = 1 << 20
	}
	if d.MaxTree <= 0 {
		d.MaxTree = 10000
	}
	if d.MaxSeen <= 0 {
		d.MaxSeen = 10000
	}
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
	d.frontierSteps -= len(d.current.prefix)
	d.prefixIdx = 0
	d.frontierCalled = false
	return true
}

// Decide follows the replay prefix, then returns FIFO at the frontier.
func (d *DPOR) Decide(dp DecisionPoint) int {
	if d.prefixIdx < len(d.current.prefix) {
		p := d.current.prefix[d.prefixIdx]
		if err := matchStep(p, dp, false); err != nil {
			ids := make([]string, len(dp.Alts))
			for i, alt := range dp.Alts {
				ids[i] = alt.ID
			}
			panic(replayDivergenceError{Step: dp.Step, Detail: fmt.Sprintf("%v; expected kind=%d node=%q alternatives=%q, got kind=%d node=%q alternatives=%q", err, p.Kind, p.Node, p.AltIDs, dp.Kind, dp.Node, ids)})
		}
		d.prefixIdx++
		return int(p.Index)
	}
	return d.frontierChoice(dp)
}

// AfterRun records the run and pushes dependent alternatives only.
func (d *DPOR) AfterRun(result RunResult) {
	if len(d.tree) >= d.MaxTree {
		d.budgetErr = ResourceLimitError{Resource: "search tree", Limit: d.MaxTree}
		return
	}
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
	d.pushDependentAlts(trace, nodeIdx, false)
	d.pushDependentAlts(trace, nodeIdx, true)
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

			if len(d.stack)+len(items) >= d.MaxFrontier {
				d.budgetErr = ResourceLimitError{Resource: "search frontier", Limit: d.MaxFrontier}
				return
			}
			if !d.reservePrefix(i + 1) {
				return
			}
			newPrefix := make(Trace, i+1)
			copy(newPrefix, trace[:i])
			newPrefix[i] = branchStep(step, alt)

			key := traceKey(newPrefix)
			if _, ok := d.seen[key]; ok {
				d.frontierSteps -= i + 1
				continue
			}
			if len(d.seen) >= d.MaxSeen {
				d.budgetErr = ResourceLimitError{Resource: "search seen", Limit: d.MaxSeen}
				return
			}
			d.seen[key] = struct{}{}
			items = append(items, dporItem{
				prefix:  newPrefix,
				nonFIFO: newNonFIFO,
				parent:  nodeIdx,
				branch:  i,
				score:   d.branchScore(trace, i, int(alt), wantGlobal),
			})
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].score < items[j].score
	})
	d.stack = append(d.stack, items...)
}

func (d *DPOR) branchScore(trace Trace, stepIdx int, alt int, wantGlobal bool) int {
	score := stepIdx
	if wantGlobal {
		score += 10_000
	}
	resource := stepAltResource(trace[stepIdx], alt)
	for _, endpoint := range d.PrioritizeEndpoints {
		if resourceTouchesEndpoint(resource, endpoint) {
			score += 1_000_000
		}
	}
	return score
}

func (d *DPOR) frontierChoice(dp DecisionPoint) int {
	if !d.frontierCalled && d.Frontier != nil {
		d.frontierCalled = true
		if choice, ok := d.Frontier(dp); ok {
			return choice
		}
	}
	if len(d.PrioritizeEndpoints) == 0 || dp.Kind != Global {
		return 0
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
	_, _, aEndpoint := splitEndpointResource(a)
	_, _, bEndpoint := splitEndpointResource(b)
	if aEndpoint && bEndpoint {
		return true
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
	if step.Kind != Global {
		return step.Node
	}
	return step.To
}

func stepAltResource(step NewStep, alt int) string {
	if alt >= 0 && alt < len(step.AltResources) {
		return step.AltResources[alt]
	}
	if step.Kind != Global {
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

func (d *DPOR) reservePrefix(steps int) bool {
	if len(d.stack) >= d.MaxFrontier || d.frontierSteps+steps > d.MaxFrontierSteps {
		d.budgetErr = ResourceLimitError{Resource: "search frontier", Limit: d.MaxFrontierSteps}
		return false
	}
	d.frontierSteps += steps
	return true
}
func (d *DPOR) SearchError() error { return d.budgetErr }
