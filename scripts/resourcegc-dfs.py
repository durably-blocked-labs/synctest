#!/usr/bin/env python3
"""Bounded prefix DFS of a one-shot resourcegc test worker.

Expected application errors are retained as outcomes and do not prune DFS.
Limits, invalid prefixes, mismatched GC, crashes and timeouts prevent exhaustion.
Every trial uses a fresh process and exclusive files. No state-based pruning.
"""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def key(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def branches(decisions, prefix_length):
    """Disjoint DFS subtrees; inherited prefix decisions never branch again."""
    for position in range(prefix_length, len(decisions)):
        decision = decisions[position]
        for alternative in reversed(range(len(decision['ordered_bgids']))):
            if alternative == decision['chosen_index']:
                continue
            prefix = [dict(item) for item in decisions[:position + 1]]
            prefix[-1]['chosen_index'] = alternative
            yield prefix


def validate(result, prefix, reference, forced_collections=0, gc_driver=False, processors=1, strict=False, stw_probes=0):
    if result.get('version') != 1 or not result.get('completed'):
        raise ValueError('worker did not complete: ' + str(result.get('status')))
    if result.get('worker_unsafe') or result.get('diverged') or result.get('decision_overflow'):
        raise ValueError('unsafe/diverged/limited worker')
    if (result.get('forced_gc_delta') != forced_collections
            or result.get('automatic_gc_delta') != 0 or result.get('total_gc_delta') != forced_collections):
        raise ValueError('measured GC treatment mismatch')
    if result.get('processors', 1) != processors:
        raise ValueError('processor count mismatch')
    if gc_driver:
        treatment = result.get('gc_treatment', {})
        if (treatment.get('requested') != forced_collections or treatment.get('completed') != forced_collections
                or len(treatment.get('calls', [])) != forced_collections or len(treatment.get('actors', [])) != 2):
            raise ValueError('GC driver did not complete requested treatment')
        for window in treatment['calls'] + treatment['actors']:
            if not 0 < window['begin_ns'] <= window['end_ns']:
                raise ValueError('invalid GC/actor timing window')
        if (treatment.get('requested_stw',0) != stw_probes or treatment.get('completed_stw',0) != stw_probes
                or len(treatment.get('stw_calls',[])) != stw_probes):
            raise ValueError('STW control did not complete requested treatment')
        for window in treatment.get('stw_calls',[]):
            if not 0 < window['begin_ns'] <= window['end_ns']:
                raise ValueError('invalid STW control window')
    decisions = result['decisions']
    if result.get('prefix_consumed') != len(prefix) or decisions[:len(prefix)] != prefix:
        raise ValueError('requested prefix not consumed identically')
    if strict and len(decisions) != len(prefix):
        raise ValueError('strict replay did not consume exactly the saved trace')
    runtime_trace = result['runtime_trace']
    for i, raw in enumerate(runtime_trace):
        ids = raw['RunqBgids'][:raw['RunqSize']]
        if (raw['Kind'] != 0 or raw['Step'] != i or not ids
                or len(ids) != raw['RunqSize'] or len(ids) > 16
                or len(ids) != len(set(ids)) or not 0 <= raw['Index'] < len(ids)
                or raw['ChosenBgid'] != ids[raw['Index']]):
            raise ValueError('invalid raw runtime record')
    hooked_steps = set()
    previous_step = -1
    for i, d in enumerate(decisions):
        ids = d['ordered_bgids']
        choice = d['chosen_index']
        if (not previous_step < d['step'] < len(runtime_trace) or not ids or len(set(ids)) != len(ids)
                or not 0 <= choice < len(ids)):
            raise ValueError('invalid decision')
        raw = runtime_trace[d['step']]
        if (raw['Index'] != choice
                or raw['RunqSize'] != len(ids) or raw['RunqBgids'][:len(ids)] != ids
                or raw['ChosenBgid'] != ids[choice]):
            raise ValueError('runtime/callback identity mismatch')
        hooked_steps.add(d['step'])
        previous_step = d['step']
        if i >= len(prefix) and choice != 0:
            raise ValueError('non-default choice outside requested prefix')
    uncontrolled = []
    for raw in runtime_trace:
        if raw['Step'] in hooked_steps:
            continue
        if raw['RunqSize'] != 1 or raw['Index'] != 0:
            raise ValueError('runtime branch occurred outside the decision hook')
        uncontrolled.append({'step':raw['Step'],'ordered_bgids':raw['RunqBgids'][:1],
                             'chosen_index':0})
    if result.get('uncontrolled_steps') != uncontrolled:
        raise ValueError('uncontrolled deterministic decisions were not retained')
    app = result['application']
    if not app['completed'] or not app['events'] or app['events'][0]['action'] != 'initial':
        raise ValueError('missing application completion or initial event')
    bug = app['worker_error'] == 'ErrClosed'
    if app['worker_error'] not in ('', 'ErrClosed') or result['application_bug'] != bug:
        raise ValueError('invalid application error classification')
    if result['status'] != ('application_bug' if bug else 'completed'):
        raise ValueError('invalid completed status')
    if 'timeline' in result:
        if result.get('observation_overflow'):
            raise ValueError('observation timeline is incomplete')
        call_stacks,actor_roles,entered = {},{},Counter()
        for ordinal,event in enumerate(result['timeline'],1):
            if event['sequence'] != ordinal:
                raise ValueError('observation sequence has a gap')
            if event['kind'] == 'schedule':
                step=event['native_step']
                if not 0 <= step < len(runtime_trace) or event['bgid'] != runtime_trace[step]['ChosenBgid']:
                    raise ValueError('timeline schedule differs from raw trace')
            elif event['kind'] == 'call':
                call=event['call']
                if not call['function'] or not call['file'] or call['line'] <= 0:
                    raise ValueError('call source was not symbolized')
                role=actor_roles.setdefault(event['bgid'],call['actor'])
                if role != call['actor']:
                    raise ValueError('call actor identity changed')
                active=call_stacks.setdefault(event['bgid'],[])
                if call['phase']=='enter':
                    active.append(call['operation'])
                    entered[call['operation']]+=1
                elif call['phase']=='return' and active and active.pop()==call['operation']:
                    pass
                else:
                    raise ValueError('unbalanced call boundaries')
            elif event['kind'] in ('gc_scan_begin', 'gc_resume_requested') and forced_collections:
                pass
            elif event['kind'] in ('preempt','preempt_request','world_stop_wait','world_stop_resumed','world_stop_continued','control_snapshot','scan_suspend','runtime_continued','gc_internal_pause'):
                pass
            else:
                raise ValueError('unexpected GC/unknown observation in zero-GC baseline')
        if any(call_stacks.values()):
            raise ValueError('unterminated calls at completed application snapshot')
        expected_calls=Counter(e['action'].split(':')[0] for e in app['events'][1:])
        for operation,count in expected_calls.items():
            if entered[operation] != count:
                raise ValueError('call markers do not match application actions')
    events = app['events']
    word = tuple(e['action'] for e in events[1:])
    if reference:
        if events[0]['state'] != reference['initial_state']:
            raise ValueError('application initial state differs from reference')
        transitions = {(key(e['before']), e['action'], key(e['after']))
                       for e in reference['transitions']}
        for before, after in zip(events, events[1:]):
            if (key(before['state']), after['action'], key(after['state'])) not in transitions:
                raise ValueError('application action/state outside reference model')
        known = {tuple(h['actions']): bool(h['error_at']) for h in reference['histories']}
        if word not in known or known[word] != bug:
            raise ValueError('application history/outcome outside reference model')
    return word, {key(e['state']) for e in events}


def run_process(command, env, cwd, log_path, timeout):
    start = time.monotonic()
    with log_path.open('xb') as output:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        # Nested watchdogs first terminate cooperatively so an intermediary
        # Python driver can reap its own independently grouped worker. Killing
        # only that driver with SIGKILL could orphan the active worker group.
        def stop_group():
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()

        def terminated(signum, _frame):
            stop_group()
            raise SystemExit(128 + signum)

        previous_handlers = {s: signal.signal(s, terminated) for s in (signal.SIGTERM, signal.SIGINT)}
        timed_out = False
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            stop_group()
            code = process.wait()
        finally:
            for signum, previous in previous_handlers.items():
                signal.signal(signum, previous)
    return {'command': command, 'returncode': code, 'timed_out': timed_out,
            'elapsed_seconds': time.monotonic() - start}


def explore(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = Path(args.worker).resolve()
    reference = json.loads(Path(args.reference).read_text()) if args.reference else None
    env = dict(os.environ)
    forced_collections = getattr(args, 'forced_collections', 0)
    gc_driver = getattr(args, 'gc_driver', False)
    processors = getattr(args, 'processors', 1)
    stw_probes = getattr(args, 'stw_probes', 0)
    env.update(GOROOT=str(Path(args.goroot).resolve()), GOTOOLCHAIN='local',
               GOMAXPROCS=str(processors), GOGC='off', GOMEMLIMIT='off', GOWORK='off',
               GOENV='off', GODEBUG='asyncpreemptoff=1')
    manifest = {'version': 1, 'worker': str(binary),
                'worker_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                'reference_sha256': hashlib.sha256(Path(args.reference).read_bytes()).hexdigest() if args.reference else None,
                'policy': 'identity-checked-prefix-dfs/default-zero/no-state-pruning',
                'max_runs': args.max_runs, 'max_seconds': args.max_seconds,
                'max_frontier': args.max_frontier, 'trial_timeout': args.trial_timeout,
                'forced_collections': forced_collections, 'gc_driver': gc_driver, 'processors': processors, 'stw_probes': stw_probes,
                'environment': {k: env[k] for k in ('GOROOT','GOTOOLCHAIN','GOMAXPROCS','GOGC','GOMEMLIMIT','GOWORK','GOENV','GODEBUG')}}
    save(output/'manifest.json', manifest)
    stack, seen = [[]], {key([])}
    runtime_words, application_words, states = set(), {}, set()
    outcomes = Counter()
    trials = 0
    first_bug = None
    start = time.monotonic()
    status, detail = 'frontier_empty', ''
    uncontrolled_baseline = None
    while stack:
        elapsed = time.monotonic() - start
        if trials >= args.max_runs or elapsed >= args.max_seconds:
            status = 'run_limit' if trials >= args.max_runs else 'time_limit'
            break
        prefix = stack.pop()
        trials += 1
        trial = output/f'{trials:06d}'
        trial.mkdir()
        request = {'version': 1, 'mode': 'explore', 'prefix': prefix,
                   'processors': processors, 'gc_driver': gc_driver, 'forced_collections': forced_collections, 'stw_probes': stw_probes}
        save(trial/'request.json', request)
        env['RESOURCEGC_REQUEST'] = str(trial/'request.json')
        env['RESOURCEGC_RESULT'] = str(trial/'result.json')
        command = [str(binary), '-test.run=^TestResourceGCWorker$', '-test.count=1', '-test.timeout=8s']
        remaining = args.max_seconds-(time.monotonic()-start)
        if remaining <= 0:
            stack.append(prefix)
            status, detail = 'time_limit', 'budget elapsed before launching prepared trial'
            save(trial/'process.json', {'launched':False,'reason':detail})
            break
        try:
            try:
                process_status = run_process(command, env, str(binary.parent), trial/'worker.log',
                                             min(args.trial_timeout, remaining))
            except OSError as error:
                process_status = {'returncode':None,'timed_out':False,'launch_error':str(error)}
            save(trial/'process.json', process_status)
            if process_status['timed_out'] or process_status['returncode'] != 0:
                raise ValueError('trial timed out or process failed')
            result = json.loads((trial/'result.json').read_text())
            word, observed_states = validate(result, prefix, reference, forced_collections, gc_driver, processors, stw_probes=stw_probes)
            if uncontrolled_baseline is None:
                uncontrolled_baseline = result['uncontrolled_steps']
            elif result['uncontrolled_steps'] != uncontrolled_baseline:
                raise ValueError('deterministic unhooked steps changed across trials')
        except (ValueError, KeyError, TypeError, OSError) as error:
            stack.append(prefix)
            status, detail = 'incomplete_trial', str(error)
            outcomes['incomplete'] += 1
            break
        trace_key = key([{'step':raw['Step'],'ordered_bgids':raw['RunqBgids'][:raw['RunqSize']],
                         'chosen_index':raw['Index'],'wait_reason':raw.get('WaitReason',0)}
                        for raw in result['runtime_trace']])
        if trace_key in runtime_words:
            stack.append(prefix)
            status, detail = 'incomplete_trial', 'duplicate complete decision trace from different DFS prefixes'
            outcomes['incomplete'] += 1
            break
        runtime_words.add(trace_key)
        states.update(observed_states)
        application_words.setdefault(word, {'witness_trial': trials, 'bug': result['application_bug']})
        outcomes['application_bug' if result['application_bug'] else 'safe'] += 1
        if result['application_bug'] and first_bug is None:
            first_bug = trials
        additions = []
        for branch in branches(result['decisions'], len(prefix)):
            branch_key = key(branch)
            if branch_key not in seen:
                seen.add(branch_key)
                additions.append(branch)
        stack.extend(additions)
        if len(stack) > args.max_frontier:
            status, detail = 'frontier_limit', 'retained every pending prefix; increase cap to resume explicitly'
            break
    summary = {'version': 1, 'status': status, 'detail': detail,
               'observed_frontier_empty': status == 'frontier_empty' and not stack,
               'exhausted_exposed_prefix_tree': status == 'frontier_empty' and not stack and forced_collections == 0 and not gc_driver,
               'scope': 'observed prefixes for this executable and treatment; GC timing is not enumerated; not all legal Go schedules',
               'forced_collections': forced_collections, 'gc_driver': gc_driver, 'processors': processors, 'stw_probes': stw_probes,
               'trials': trials, 'unique_runtime_decision_traces': len(runtime_words),
               'unique_application_traces': len(application_words),
               'bug_application_traces': sum(w['bug'] for w in application_words.values()),
               'reachable_projected_states': len(states),
               'reference_application_traces': len(reference['histories']) if reference else None,
               'reference_states': len(reference['states']) if reference else None,
               'outcomes': dict(outcomes), 'first_bug_trial': first_bug,
               'deterministic_uncontrolled_steps': uncontrolled_baseline,
               'pending_prefixes': len(stack), 'elapsed_seconds': time.monotonic()-start,
               'application_witnesses': [{'actions': list(w), **data} for w,data in application_words.items()]}
    save(output/'pending-prefixes.json', stack)
    save(output/'summary.json', summary)
    print(json.dumps(summary, indent=2))
    return 0 if summary['observed_frontier_empty'] else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--worker', required=True)
    p.add_argument('--goroot', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--reference')
    p.add_argument('--max-runs', type=int, default=256)
    p.add_argument('--max-seconds', type=float, default=120)
    p.add_argument('--max-frontier', type=int, default=10000)
    p.add_argument('--trial-timeout', type=float, default=5)
    p.add_argument('--processors', type=int, choices=(1, 2), default=1)
    p.add_argument('--gc-driver', action='store_true')
    p.add_argument('--forced-collections', type=int, choices=(0, 1, 2, 4, 8), default=0)
    p.add_argument('--stw-probes', type=int, choices=(0,1), default=0)
    args = p.parse_args()
    if min(args.max_runs,args.max_seconds,args.max_frontier,args.trial_timeout) <= 0:
        p.error('all limits must be positive')
    if (args.gc_driver and args.processors != 2) or ((args.forced_collections or args.stw_probes) and not args.gc_driver):
        p.error('GC driver requires two processors; collections require the driver')
    return explore(args)


if __name__ == '__main__':
    raise SystemExit(main())
