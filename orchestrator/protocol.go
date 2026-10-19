package orchestrator

import (
	"context"
	"fmt"
	"sync"
)

// Publications is bookkeeping, not a detached-callback progress barrier.
// Qualified publishers register and complete while attached, without blocking
// or detaching between Begin and complete. At idle, Pending MUST be zero.
type Publications struct {
	mu       sync.Mutex
	pending  int
	changed  *sync.Cond
	sequence uint64
}

func (p *Publications) Begin(node string) (occurrence string, complete func()) {
	p.mu.Lock()
	p.pending++
	p.sequence++
	occurrence = fmt.Sprintf("%s:%d", node, p.sequence)
	p.mu.Unlock()
	var once sync.Once
	return occurrence, func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.pending--
			if p.pending == 0 && p.changed != nil {
				p.changed.Broadcast()
			}
		})
	}
}
func (p *Publications) Wait(ctx context.Context) error {
	// Do not allocate a notification channel in Begin: Begin runs attached,
	// which would tag that channel with the bubble and forbid coordinator reads.
	p.mu.Lock()
	if p.pending == 0 {
		p.mu.Unlock()
		return nil
	}
	if p.changed == nil {
		p.changed = sync.NewCond(&p.mu)
	}
	finished := make(chan struct{}) // Wait is coordinator-only, outside the bubble.
	stop := context.AfterFunc(ctx, func() { p.mu.Lock(); p.changed.Broadcast(); p.mu.Unlock(); close(finished) })
	for p.pending > 0 && ctx.Err() == nil {
		p.changed.Wait()
	}
	var err error
	if p.pending > 0 {
		err = ctx.Err()
	}
	p.mu.Unlock()
	if !stop() {
		<-finished
	} // join a started cancellation callback before returning
	return err
}

const AttachedPublicationV1 = "attached-bounded/v1"

// AcknowledgedTransport is the qualified attached-publication boundary.
// WaitPublications alone cannot prove a detached callback can make progress
// while its owner is frozen. Detached/legacy publication is unsupported here.
type AcknowledgedTransport interface {
	NodeTransport
	PublicationPolicy() string
	PendingPublications() int
}

func (p *Publications) Pending() int { p.mu.Lock(); defer p.mu.Unlock(); return p.pending }

type UnsupportedTransportError struct{ Node, Policy string }

func (e UnsupportedTransportError) Error() string {
	return fmt.Sprintf("transport %q unsupported publication policy %q; require %s", e.Node, e.Policy, AttachedPublicationV1)
}

func validateTransport(addr string, tr NodeTransport) error {
	a, ok := tr.(AcknowledgedTransport)
	if !ok {
		return UnsupportedTransportError{Node: addr, Policy: "legacy/unacknowledged"}
	}
	if a.PublicationPolicy() != AttachedPublicationV1 {
		return UnsupportedTransportError{Node: addr, Policy: a.PublicationPolicy()}
	}
	if a.PendingPublications() != 0 {
		return fmt.Errorf("transport %q has incomplete attached publication while frozen", addr)
	}
	return nil
}

// DeliveryReceipt distinguishes causal runtime readiness from target work.
// WaitReady returns only after external reattachment has entered the runtime's
// owner queue. Consumed closes after the attached bridge accepts the envelope.
// A no-work/drop delivery may return nil channels; it must still return an error
// for a failed delivery. No log-growth or host-time retry defines consumption.
type DeliveryReceipt struct {
	// WaitReady must synchronously await runtime owner-queue registration.
	// Runtime Ready.Wait is not cancellable: a stuck wait requires process kill,
	// never a detached waiter goroutine followed by worker reuse.
	WaitReady func()
	Consumed  <-chan struct{}
	Completed <-chan error    // attached processing/forwarding result, if supplied
	Cancelled <-chan struct{} // explicit transport-close/drop terminal acknowledgement
}

func waitConsumption(ctx context.Context, c *nodeCtrl, r DeliveryReceipt) error {
	if r.Consumed == nil && r.Completed == nil {
		return nil
	}
	select {
	case err := <-r.Completed:
		return err
	case <-r.Consumed:
		return nil
	case <-r.Cancelled:
		return nil
	case <-c.done:
		if c.unsafe {
			panic(nodeFailure{c})
		}
		select {
		case err := <-r.Completed:
			return err
		case <-r.Consumed:
			return nil
		case <-r.Cancelled:
			return nil
		default:
			return fmt.Errorf("node completed before delivery consumption")
		}
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// ConfigurationError rejects a distributed trial before any target starts.
// One P per node plus a spare host/coordinator P is required; no automatic bump.
type ConfigurationError struct{ Available, Required int }

func (e ConfigurationError) Error() string {
	return fmt.Sprintf("orchestrator requires GOMAXPROCS >= %d (available %d): one owner P per node plus one host P", e.Required, e.Available)
}

type ResourceLimitError struct {
	Resource string
	Limit    int
}

func (e ResourceLimitError) Error() string {
	return fmt.Sprintf("%s limit exceeded (%d)", e.Resource, e.Limit)
}

// TrialLimits are host safety budgets, never target scheduling alternatives.
type TrialLimits struct {
	Decisions, PendingOperations int
	TraceBytes                   int
}

func defaultTrialLimits() TrialLimits {
	return TrialLimits{Decisions: 512, PendingOperations: 4096, TraceBytes: 8 << 20}
}
