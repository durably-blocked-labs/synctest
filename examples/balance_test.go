package examples

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

// TestAccountSetup shows a missing-synchronization bug: one goroutine
// creates user accounts while another reads them. If the reader runs
// first it sees zero balances.
//
// Run: GODEBUG=asyncpreemptoff=1 ../go/bin/go test -v -run TestAccountSetup .
func TestAccountSetup(t *testing.T) {
	explorer.Test(t, func(t *testing.T) {
		accounts := make(map[string]int)
		done := make(chan struct{}, 2)

		go func() {
			total := accounts["alice"] + accounts["bob"]
			if total != 150 {
				t.Errorf("total = %d, want 150", total)
			}
			done <- struct{}{}
		}()

		go func() {
			accounts["alice"] = 100
			accounts["bob"] = 50
			done <- struct{}{}
		}()

		<-done
		<-done
	})
}
