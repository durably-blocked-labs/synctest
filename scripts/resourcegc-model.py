#!/usr/bin/env python3
"""Pure finite application-action enumeration, not a Go/runtime experiment."""
from dataclasses import dataclass, replace, asdict
from collections import Counter
import json


@dataclass(frozen=True)
class State:
    worker: str = 'claim'
    cleanup: str = 'check'
    in_use: bool = False
    closed: bool = False
    close_decision: str = 'unset'
    error: str = ''
    read_value: int = -1
    computed_value: int = -1
    result: int = 0


def successors(s):
    if s.worker == 'claim':
        if s.closed:
            yield 'claim:ErrClosed', replace(s, worker='done', error='ErrClosed')
        else:
            yield 'claim', replace(s, worker='read', in_use=True)
    elif s.worker == 'read':
        if s.closed:
            yield 'read:ErrClosed', replace(s, worker='release', error='ErrClosed')
        else:
            yield 'read', replace(s, worker='compute', read_value=21)
    elif s.worker == 'compute':
        yield 'compute', replace(s, worker='commit', computed_value=2*s.read_value)
    elif s.worker == 'commit':
        if s.closed:
            yield 'commit:ErrClosed', replace(s, worker='release', error='ErrClosed')
        else:
            yield 'commit', replace(s, worker='release', result=s.computed_value)
    elif s.worker == 'release':
        yield 'release', replace(s, worker='done', in_use=False)
    if s.cleanup == 'check':
        if s.in_use:
            yield 'check:skip', replace(s, cleanup='done', close_decision='no')
        else:
            yield 'check:close', replace(s, cleanup='close', close_decision='yes')
    elif s.cleanup == 'close':
        yield 'close', replace(s, cleanup='done', closed=True)


def enumerate_model():
    states, terminals, edges = set(), set(), set()
    histories = []
    def visit(s, word):
        states.add(s)
        options = list(successors(s))
        if not options:
            assert s.worker == s.cleanup == 'done'
            assert not s.in_use
            assert bool(s.error) == (s.result == 0)
            terminals.add(s)
            error_at = next((label.split(':')[0] for label in word if label.endswith(':ErrClosed')), None)
            histories.append({'actions': word, 'error_at': error_at,
                              'final_state': asdict(s)})
        for label, t in options:
            edges.add((s, label, t))
            visit(t, word+(label,))
    visit(State(), ())
    errors = Counter(h['error_at'] or 'none' for h in histories)
    assert len(histories) == 11 and errors == {'none': 7, 'claim': 1, 'read': 1, 'commit': 2}
    return {'scope':'exact declared abstract lifecycle model; no runtime/GC execution',
            'complete_application_histories':len(histories),
            'failing_application_histories':sum(bool(h['error_at']) for h in histories),
            'successful_application_histories':errors['none'],
            'reachable_application_states':len(states),
            'distinct_terminal_states':len(terminals),
            'labeled_graph_edges':len(edges),
            'error_locations':dict(errors),
            'initial_state':asdict(State()),
            'states':[asdict(s) for s in sorted(states, key=repr)],
            'transitions':[{'before':asdict(s),'action':label,'after':asdict(t)}
                           for s,label,t in sorted(edges, key=repr)],
            'histories':histories}


if __name__ == '__main__':
    print(json.dumps(enumerate_model(), indent=2))
