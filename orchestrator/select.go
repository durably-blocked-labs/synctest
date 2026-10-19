package orchestrator

import (
	"fmt"
	"testing/synctest"
)

// SelectDecisionPoint maps enabled-array positions to runtime partition indices.
// Site is main-module text-relative and requires an identical executable pin.
// Neither cases nor the actor are represented as runnable goroutine choices.
func SelectDecisionPoint(state synctest.SelectState, step int, node string) DecisionPoint {
	dp := DecisionPoint{Kind: Select, Step: step, Node: node, Site: fmt.Sprintf("text+%x", state.Site), Actor: state.Goroutine}
	for i := int32(0); i < state.CaseN; i++ {
		c := state.Cases[i]
		dp.Alts = append(dp.Alts, Alt{ID: fmt.Sprintf("select:%s:B%d:case%d", dp.Site, dp.Actor, c), CaseIndex: c})
	}
	return dp
}
