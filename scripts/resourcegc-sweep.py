#!/usr/bin/env python3
"""Bounded 0/1/2/4/8 forced-GC characterization; preserve failures verbatim."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time

spec = importlib.util.spec_from_file_location('resource_dfs', Path(__file__).with_name('resourcegc-dfs.py'))
dfs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dfs)


def application_key(result):
    return dfs.key(result['application'])


def schedule_key(result):
    return dfs.key([{'step': r['Step'], 'ids': r['RunqBgids'][:r['RunqSize']],
                     'choice': r['Index'], 'wait': r.get('WaitReason', 0)}
                    for r in result['runtime_trace']])


def replay_summary(rows):
    return {'attempts': len(rows), 'hook_acceptances': sum(r['valid'] for r in rows),
            'full_matches': sum(r['valid'] and r['strict_schedule_match'] and r['same_application'] for r in rows),
            'same_native_different_application': sum(r['valid'] and r['strict_schedule_match'] and r['same_application'] is False for r in rows)}


def characterization_passed(conditions):
    """Require complete expected samples; never let an empty/partial run pass."""
    if [c['condition'] for c in conditions] != [0, 1, 2, 4, 8]:
        return False
    for c in conditions:
        if (c.get('dfs', {}).get('status') != 'frontier_empty'
                or not c['sample_replay_trials']
                or c['sample_replay_trials'] != c['expected_sample_trials']
                or c['baseline_replay_attempts'] != c['expected_baseline_replays']
                or c['valid_trials'] != c['sample_replay_trials']
                or not c['baseline_replay_attempts']
                or c['baseline_replay_full_matches'] != c['baseline_replay_attempts']):
            return False
        within = c['within_treatment_replays']
        if within['attempts'] != c['expected_within_replays']:
            return False
        if c['condition'] and (not within['attempts'] or within['full_matches'] != within['attempts']):
            return False
    return True


def observation_summary(result):
    treatment = result.get('gc_treatment') or {}
    actors = treatment.get('actors', [])
    calls = treatment.get('calls', [])
    overlap = sum(any(c['begin_ns'] < a['end_ns'] and a['begin_ns'] < c['end_ns']
                      for a in actors) for c in calls)
    completed_during = sum(any(a['begin_ns'] <= c['end_ns'] <= a['end_ns']
                              for a in actors) for c in calls)
    active, scans, resume_scans = {}, [], 0
    for e in result.get('timeline', []):
        if e['kind'] == 'call' and e.get('call', {}).get('operation') in ('runWorker', 'runCleanup'):
            if e['call']['phase'] == 'enter':
                active[e['bgid']] = e['call']['actor']
            else:
                active.pop(e['bgid'], None)
        elif e['kind'] == 'gc_scan_begin' and e['bgid'] in active:
            scans.append({'sequence': e['sequence'], 'actor': active[e['bgid']],
                          'bgid': e['bgid'], 'stopped': e['stopped'],
                          'gc_cycle_counter': e['gc_cycle'], 'site': e.get('captured_site'),
                          'status_before_suspend': e['status_before_suspend']})
            resume_scans += int(e['stopped'])
    return {'requested': treatment.get('requested'), 'completed': treatment.get('completed'),
            'gc_call_windows_overlapping_actor_lifetimes': overlap,
            'gc_calls_completed_during_actor_lifetimes': completed_during,
            'scans_during_actor_calls': scans, 'scans_requiring_resume': resume_scans,
            'scope': 'call overlap is not GC-phase overlap; active stacks may be parked; Stopped does not alone prove initially running'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--worker', required=True)
    p.add_argument('--goroot', required=True)
    p.add_argument('--reference', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--max-seconds', type=float, default=180)
    p.add_argument('--dfs-runs', type=int, default=32)
    p.add_argument('--dfs-seconds', type=float, default=15)
    p.add_argument('--repeats', type=int, default=5)
    args = p.parse_args()
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    worker = Path(args.worker).resolve()
    reference = json.loads(Path(args.reference).read_text())
    env = dict(os.environ, GOROOT=str(Path(args.goroot).resolve()), GOTOOLCHAIN='local',
               GOMAXPROCS='2', GOGC='off', GOMEMLIMIT='off', GOWORK='off', GOENV='off',
               GODEBUG='asyncpreemptoff=1')
    dfs.save(root/'manifest.json', {'worker': str(worker), 'worker_sha256': hashlib.sha256(worker.read_bytes()).hexdigest(),
             'goroot': env['GOROOT'], 'reference_sha256': hashlib.sha256(Path(args.reference).read_bytes()).hexdigest(),
             'conditions': [0,1,2,4,8], 'processors': 2, 'driver': 'host goroutine triggered by first actor entry',
             'limits': vars(args), 'environment': {k:env[k] for k in ('GOROOT','GOTOOLCHAIN','GOMAXPROCS','GOGC','GOMEMLIMIT','GOWORK','GOENV','GODEBUG')}})
    start = time.monotonic()
    records, conditions, baseline = [], [], []

    def trial(directory, count, prefix, mode, witness=None, comparison='default'):
        remaining = args.max_seconds - (time.monotonic()-start)
        if remaining <= 0:
            return None
        directory.mkdir()
        request = {'version':1, 'mode':mode, 'prefix':prefix, 'processors':2,
                   'gc_driver':True, 'forced_collections':count}
        dfs.save(directory/'request.json', request)
        trial_env = dict(env, RESOURCEGC_REQUEST=str(directory/'request.json'), RESOURCEGC_RESULT=str(directory/'result.json'))
        command = [str(worker), '-test.run=^TestResourceGCWorker$', '-test.count=1', '-test.timeout=8s']
        process = dfs.run_process(command, trial_env, str(worker.parent), directory/'worker.log', min(5, remaining))
        dfs.save(directory/'process.json', process)
        row = {'condition':count, 'path':str(directory.relative_to(root)), 'comparison':comparison,
               'process':process, 'valid':False, 'strict_schedule_match':False, 'same_application':None}
        try:
            if process['returncode'] != 0 or process['timed_out']:
                raise ValueError('worker process failed or timed out')
            result = json.loads((directory/'result.json').read_text())
            row['worker_status'] = result['status']
            row['application_completed'] = result['application']['completed']
            row['cause'] = result.get('cause')
            row['observations'] = observation_summary(result)
            dfs.validate(result, prefix, reference, count, True, 2, strict=mode=='strict')
            row['valid'] = True
            row['application_bug'] = result['application_bug']
            row['actions'] = [e['action'] for e in result['application']['events'][1:]]
            if witness is not None:
                row['strict_schedule_match'] = schedule_key(result) == schedule_key(witness)
                row['same_application'] = application_key(result) == application_key(witness)
        except (ValueError, KeyError, TypeError, OSError) as error:
            row['validation_error'] = str(error)
        dfs.save(directory/'comparison.json', row)
        records.append(row)
        return row

    for count in (0,1,2,4,8):
        directory = root/f'gc-{count:02d}'
        directory.mkdir()
        remaining = args.max_seconds-(time.monotonic()-start)
        if remaining <= 0:
            conditions.append({'condition':count, 'status':'budget_not_run'})
            continue
        command = [sys.executable, str(Path(__file__).with_name('resourcegc-dfs.py').resolve()),
                   '--worker',str(worker),'--goroot',env['GOROOT'],'--reference',str(Path(args.reference).resolve()),
                   '--output',str(directory/'dfs'),'--processors','2','--gc-driver','--forced-collections',str(count),
                   '--max-runs',str(args.dfs_runs),'--max-seconds',str(min(args.dfs_seconds,remaining)), '--trial-timeout','5']
        process = dfs.run_process(command, env, str(worker.parent), directory/'dfs.log', min(args.dfs_seconds+2,remaining))
        dfs.save(directory/'dfs-process.json', process)
        summary_path = directory/'dfs/summary.json'
        summary = json.loads(summary_path.read_text()) if summary_path.exists() else {'status':'process_failed'}
        witnesses = []
        for entry in summary.get('application_witnesses', [])[:8]:
            path = directory/'dfs'/f"{entry['witness_trial']:06d}"/'result.json'
            witnesses.append(json.loads(path.read_text()))
        if count == 0:
            baseline = witnesses
        conditions.append({'condition':count, 'dfs':summary,
                           'expected_sample_trials':args.repeats*(1+len(baseline))+(3*len(witnesses) if count else 0),
                           'expected_baseline_replays':args.repeats*len(baseline),
                           'expected_within_replays':3*len(witnesses) if count else 0})
        print(f'GC={count}: DFS {summary.get("status")}, {summary.get("unique_application_traces",0)} application traces', flush=True)
        for repeat in range(1,args.repeats+1):
            trial(directory/f'default-{repeat:02d}', count, [], 'explore')
        # Replaying a baseline witness under GC tests the same requested schedule.
        for i,witness in enumerate(baseline,1):
            for repeat in range(1,args.repeats+1):
                trial(directory/f'baseline-{i:02d}-r{repeat:02d}', count, witness['decisions'], 'strict', witness, 'baseline_replay')
        # Independently test replay within each treatment; retain divergences.
        if count:
            for i,witness in enumerate(witnesses,1):
                for repeat in range(1,4):
                    trial(directory/f'own-{i:02d}-r{repeat:02d}', count, witness['decisions'], 'strict', witness, 'within_treatment_replay')
        own = [r for r in records if r['condition']==count]
        print(f'GC={count}: {sum(r["valid"] for r in own)}/{len(own)} valid sample/replay trials',flush=True)

    summaries=[]
    for condition in conditions:
        count=condition['condition']
        own=[r for r in records if r['condition']==count]
        valid=[r for r in own if r['valid']]
        replay=[r for r in own if r['comparison']=='baseline_replay']
        within=[r for r in own if r['comparison']=='within_treatment_replay']
        observations=[r['observations'] for r in own if 'observations' in r]
        summaries.append({**condition,'sample_replay_trials':len(own),'valid_trials':len(valid),
            'invalid_statuses':dict(Counter(r.get('worker_status',r.get('validation_error')) for r in own if not r['valid'])),
            'baseline_replay_attempts':len(replay),
            'baseline_replay_full_matches':sum(r['valid'] and r['strict_schedule_match'] and r['same_application'] for r in replay),
            'within_treatment_replay_attempts':len(within),
            'within_treatment_replays':replay_summary(within),
            'invalid_after_application_completion':sum(not r['valid'] and r.get('application_completed',False) for r in own),
            'valid_sample_bug_trials':sum(r['application_bug'] for r in valid),
            'valid_sample_application_words':sorted({tuple(r['actions']) for r in valid}),
            'requested_collections_in_observed_trials':sum(o['requested'] or 0 for o in observations),
            'completed_collections_in_observed_trials':sum(o['completed'] or 0 for o in observations),
            'gc_call_windows_overlapping_actors':sum(o['gc_call_windows_overlapping_actor_lifetimes'] for o in observations),
            'gc_calls_completed_during_actors':sum(o['gc_calls_completed_during_actor_lifetimes'] for o in observations),
            'active_actor_scan_events':sum(len(o['scans_during_actor_calls']) for o in observations),
            'active_actor_scans_requiring_resume':sum(o['scans_requiring_resume'] for o in observations)})
    passed = characterization_passed(summaries)
    summary={'passed':passed,'elapsed_seconds':time.monotonic()-start,'conditions':summaries,
             'scope':'bounded characterization, not enumeration of GC timings or proof of invariance',
             'records':records}
    dfs.save(root/'summary.json',summary)
    lines=['# Incremental GC characterization','','Matched two-processor configuration with a host driver in every condition.',
           'GC call overlap is not evidence of running-actor interruption. GC timing is not enumerated.','',
           '| Requested GC/run | DFS status | Valid sample/replay trials | Baseline strict matches | Call windows overlapping actors | Active actor scans | Scans requiring resume |',
           '| ---: | --- | ---: | ---: | ---: | ---: | ---: |']
    for s in summaries:
        lines.append(f"| {s['condition']} | {s.get('dfs',{}).get('status',s.get('status'))} | {s['valid_trials']}/{s['sample_replay_trials']} | {s['baseline_replay_full_matches']}/{s['baseline_replay_attempts']} | {s['gc_call_windows_overlapping_actors']} | {s['active_actor_scan_events']} | {s['active_actor_scans_requiring_resume']} |")
    (root/'summary.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps({'passed':passed,'elapsed_seconds':summary['elapsed_seconds'],'conditions':summaries},indent=2),flush=True)
    return 0 if passed else 2


if __name__=='__main__':
    sys.exit(main())
