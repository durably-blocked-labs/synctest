// Package replayerscheduler demonstrates finding a concurrency bug by
// exploring scheduling interleavings.
//
// The program simulates a bank account where withdrawal requests must
// wait for approval before executing. The bug: the balance check happens
// BEFORE the approval gate, so multiple goroutines can pass the check
// with a stale balance and then all subtract — overdrawing the account.
//
// Under FIFO+priority scheduling, the approver goroutine (created last → runnext)
// runs first and opens the gate before any withdrawer starts. The
// withdrawers then run sequentially, each seeing the updated balance.
//
// Under non-FIFO scheduling, withdrawers run first, all see balance=100,
// all block at the gate. When the approver opens it, they all subtract
// — resulting in a negative balance.
package replayerscheduler

import "sync"

// Account holds a balance that can be withdrawn from.
type Account struct {
	Balance int
}

// Withdraw checks if the balance is sufficient, waits for approval,
// then subtracts. Returns true if the withdrawal was executed.
//
// BUG: The balance check happens before the approval gate. If multiple
// goroutines pass the check before any subtracts, they will all subtract
// from the stale balance, potentially overdrawing the account.
//
// The correct implementation would re-check the balance after approval.
func (a *Account) Withdraw(amount int, approved <-chan struct{}) bool {
	if a.Balance >= amount {
		<-approved // wait for approval — YIELD POINT
		a.Balance -= amount
		return true
	}
	return false
}

// Run creates an account with balance=100, launches two withdrawal
// requests for 60 each (both guarded by an approval gate), and an
// approver that opens the gate.
//
// Returns the final balance and the number of executed withdrawals.
func Run() (balance int, withdrawals int) {
	acct := &Account{Balance: 100}
	gate := make(chan struct{})

	var mu sync.Mutex
	var count int
	var wg sync.WaitGroup
	wg.Add(3) // 2 withdrawers + 1 approver

	// Withdrawer 1 — created first, deepest in runq.
	go func() {
		defer wg.Done()
		if acct.Withdraw(60, gate) {
			mu.Lock()
			count++
			mu.Unlock()
		}
	}()

	// Withdrawer 2 — created second.
	go func() {
		defer wg.Done()
		if acct.Withdraw(60, gate) {
			mu.Lock()
			count++
			mu.Unlock()
		}
	}()

	// Approver — created last → runnext → runs first under FIFO.
	go func() {
		defer wg.Done()
		close(gate) // open the approval gate for all waiters
	}()

	wg.Wait()
	return acct.Balance, count
}
