// etcd#7492 — RWMutex + Ticker + Channel Deadlock (3+ goroutines)
// https://github.com/etcd-io/etcd/pull/7492
//
// G1 holds simpleTokensMu.Lock and blocks sending on addSimpleTokenCh.
// G2 (stk.run) receives from addSimpleTokenCh, then ticker fires and
// deleteTokenFunc tries to acquire simpleTokensMu.Lock.
// Deadlock: G1 holds lock + blocked on channel, G2 needs lock in ticker handler.
// Flaky: 40/100
package etcd7492

import (
	"sync"
	"testing"
	"time"
)

type simpleTokenTTLKeeper struct {
	tokens           map[string]time.Time
	addSimpleTokenCh chan struct{}
	stopCh           chan chan struct{}
	deleteTokenFunc  func(string)
}

func newSimpleTokenTTLKeeper(deletefunc func(string)) *simpleTokenTTLKeeper {
	stk := &simpleTokenTTLKeeper{
		tokens:           make(map[string]time.Time),
		addSimpleTokenCh: make(chan struct{}, 1),
		stopCh:           make(chan chan struct{}),
		deleteTokenFunc:  deletefunc,
	}
	go stk.run()
	return stk
}

func (tm *simpleTokenTTLKeeper) run() {
	tokenTicker := time.NewTicker(time.Nanosecond)
	defer tokenTicker.Stop()
	for {
		select {
		case <-tm.addSimpleTokenCh:
			tm.tokens["1"] = time.Now()
		case <-tokenTicker.C:
			for t := range tm.tokens {
				tm.deleteTokenFunc(t)
				delete(tm.tokens, t)
			}
		case waitCh := <-tm.stopCh:
			waitCh <- struct{}{}
			return
		}
	}
}

func (tm *simpleTokenTTLKeeper) addSimpleToken() {
	tm.addSimpleTokenCh <- struct{}{}
}

func (tm *simpleTokenTTLKeeper) stop() {
	waitCh := make(chan struct{})
	tm.stopCh <- waitCh
	<-waitCh
	close(tm.stopCh)
}

type tokenSimple struct {
	simpleTokenKeeper *simpleTokenTTLKeeper
	simpleTokensMu    sync.RWMutex
}

func (t *tokenSimple) assignSimpleTokenToUser() {
	t.simpleTokensMu.Lock()
	t.simpleTokenKeeper.addSimpleToken()
	t.simpleTokensMu.Unlock()
}

func newDeleterFunc(t *tokenSimple) func(string) {
	return func(_ string) {
		t.simpleTokensMu.Lock()
		defer t.simpleTokensMu.Unlock()
	}
}

func (t *tokenSimple) enable() {
	t.simpleTokenKeeper = newSimpleTokenTTLKeeper(newDeleterFunc(t))
}

func (t *tokenSimple) disable() {
	if t.simpleTokenKeeper != nil {
		t.simpleTokenKeeper.stop()
		t.simpleTokenKeeper = nil
	}
	t.simpleTokensMu.Lock()
	t.simpleTokensMu.Unlock()
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	ts := &tokenSimple{}
	ts.enable()
	defer ts.disable()
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			ts.assignSimpleTokenToUser()
		}()
	}
	wg.Wait()
}
