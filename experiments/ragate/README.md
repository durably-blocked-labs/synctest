# RA notification race

This is a synthetic Ricart–Agrawala-style lock fixture. It contains an injected
notification-before-ownership bug and a shared in-process KV primitive for the
planned three-client experiment. It has no distributed Bubbles adapter or GC
experiment driver yet.

Each client requests the lock once. Requests use Lamport timestamps and node IDs
for priority. Lower-priority requests are deferred until release.

The faulty last-reply handler closes a local gate while state remains `Wanted`.
Both App and a deferred-reply helper wake. App records `Held`; the helper checks
only `Held` and can therefore grant deferred permission while still `Wanted` if
it inspects the state first. Publishing `Held` before opening the gate is the
corrected ordering to compare against.

`lock_test.go` checks selected protocol phases in a deterministic sequence. These
are unit checks of notification, premature permission and release behavior. They
do not demonstrate that Bubbles explores either concurrent ordering, strict
replay, overlapping critical sections, a lost counter update, or any GC effect.

The revision removes the historical handler's explicit `Wanted -> Released`
transition and changes the flusher guard. Historical traces and run counts do
not describe this revision. Previous implementations remain in Git history.

Before running a distributed experiment, define the finite transition model,
state observations, scheduling bounds and property monitor. The adapter must use
the existing attached publication and acknowledged-delivery contract; the
archived detached adapter is deliberately not copied here.
