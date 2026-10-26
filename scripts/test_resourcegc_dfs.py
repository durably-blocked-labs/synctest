"""Host-only DFS tests using a synthetic finite decision-tree worker."""
import argparse
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('dfs', Path(__file__).with_name('resourcegc-dfs.py'))
dfs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dfs)
spec_sweep = importlib.util.spec_from_file_location('sweep', Path(__file__).with_name('resourcegc-sweep.py'))
sweep = importlib.util.module_from_spec(spec_sweep)
spec_sweep.loader.exec_module(sweep)

FAKE_WORKER = '''#!/usr/bin/env python3
import json, os
r=json.load(open(os.environ['RESOURCEGC_REQUEST']))
p=r['prefix']; ds=[]
for i in range(2):
 ids=[1,2] if i==0 else ([3,4,5] if ds[0]['chosen_index']==0 else [6,7])
 choice=p[i]['chosen_index'] if i<len(p) else 0
 ds.append({'step':i,'ordered_bgids':ids,'chosen_index':choice})
bad=ds[0]['chosen_index']==1
raw=[{'Kind':0,'Step':d['step'],'Index':d['chosen_index'],'RunqSize':len(d['ordered_bgids']),
      'RunqBgids':d['ordered_bgids']+[0]*(16-len(d['ordered_bgids'])),
      'ChosenBgid':d['ordered_bgids'][d['chosen_index']]} for d in ds]
o={'version':1,'status':'application_bug' if bad else 'completed','completed':True,
   'application_bug':bad,'decisions':ds,'runtime_trace':raw,'prefix_consumed':len(p),
   'forced_gc_delta':0,'automatic_gc_delta':0,'total_gc_delta':0,
   'worker_unsafe':False,'diverged':False,'decision_overflow':False,'uncontrolled_steps':[],
   'application':{'completed':True,'worker_error':'ErrClosed' if bad else '',
                  'events':[{'action':'initial','state':{}},{'action':'finish','state':{}}]}}
with open(os.environ['RESOURCEGC_RESULT'],'x') as f:json.dump(o,f)
'''


class DFSTests(unittest.TestCase):
    def test_accepted_schedule_with_different_application_is_not_reproduced(self):
        rows=[{'valid':True,'strict_schedule_match':True,'same_application':False},
              {'valid':False,'strict_schedule_match':False,'same_application':None}]
        summary=sweep.replay_summary(rows)
        self.assertEqual(summary,{'attempts':2,'hook_acceptances':1,'full_matches':0,
                                  'same_native_different_application':1})

    def test_sweep_rejects_partial_or_mismatched_results(self):
        rows = [{'condition': count, 'dfs': {'status': 'frontier_empty'},
                 'sample_replay_trials': 10, 'valid_trials': 10, 'expected_sample_trials': 10,
                 'expected_baseline_replays': 2, 'expected_within_replays': 2,
                 'baseline_replay_attempts': 2, 'baseline_replay_full_matches': 2,
                 'within_treatment_replays': {'attempts': 2, 'full_matches': 2}}
                for count in [0, 1, 2, 4, 8]]
        self.assertTrue(sweep.characterization_passed(rows))
        self.assertFalse(sweep.characterization_passed(rows[:-1]))
        rows[2]['baseline_replay_full_matches'] = 1
        self.assertFalse(sweep.characterization_passed(rows))
        rows[2]['baseline_replay_full_matches'] = 2
        rows[2]['dfs']['status'] = 'run_limit'
        self.assertFalse(sweep.characterization_passed(rows))
        rows[2]['dfs']['status'] = 'frontier_empty'
        rows[-1]['expected_sample_trials'] = 11
        self.assertFalse(sweep.characterization_passed(rows))

    def test_outer_watchdog_reaps_nested_worker_group(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name)
            child_code="import os,time;open(%r,'w').write(str(os.getpid()));time.sleep(20)" % str(root/'child.pid')
            middle_code=("import importlib.util,os;from pathlib import Path;"
                "s=importlib.util.spec_from_file_location('d',%r);d=importlib.util.module_from_spec(s);s.loader.exec_module(d);"
                "d.run_process(%r,dict(os.environ),%r,Path(%r),10)") % (
                    str(Path(__file__).with_name('resourcegc-dfs.py').resolve()),
                    [sys.executable,'-c',child_code], str(root),str(root/'inner.log'))
            status=dfs.run_process([sys.executable,'-c',middle_code],dict(os.environ),str(root),root/'outer.log',1)
            self.assertTrue(status['timed_out'])
            child_pid=int((root/'child.pid').read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid,0)

    def test_collection_after_actors_is_not_overlap(self):
        result={'gc_treatment': {'requested':1,'completed':1,
                'calls':[{'begin_ns':30,'end_ns':40}],
                'actors':[{'begin_ns':10,'end_ns':20},{'begin_ns':15,'end_ns':25}]}, 'timeline':[]}
        observed=sweep.observation_summary(result)
        self.assertEqual(observed['gc_call_windows_overlapping_actor_lifetimes'],0)
        self.assertEqual(observed['gc_calls_completed_during_actor_lifetimes'],0)

    def test_call_overlap_and_parked_scan_are_distinct_from_resume(self):
        enter={'kind':'call','bgid':3,'call':{'operation':'runWorker','phase':'enter','actor':'worker'}}
        scan={'kind':'gc_scan_begin','bgid':3,'sequence':2,'stopped':False,
              'gc_cycle':4,'status_before_suspend':4}
        leave={'kind':'call','bgid':3,'call':{'operation':'runWorker','phase':'return','actor':'worker'}}
        result={'gc_treatment': {'requested':1,'completed':1,
                'calls':[{'begin_ns':15,'end_ns':40}],
                'actors':[{'begin_ns':10,'end_ns':20}]}, 'timeline':[enter,scan,leave]}
        observed=sweep.observation_summary(result)
        self.assertEqual(observed['gc_call_windows_overlapping_actor_lifetimes'],1)
        self.assertEqual(observed['gc_calls_completed_during_actor_lifetimes'],0)
        self.assertEqual(len(observed['scans_during_actor_calls']),1)
        self.assertEqual(observed['scans_requiring_resume'],0)

    def exercise(self, limit, executable=True):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            worker = root/'worker'
            worker.write_text(FAKE_WORKER)
            worker.chmod(0o700 if executable else 0o600)
            args=argparse.Namespace(output=str(root/'run'),worker=str(worker),reference=None,
                goroot=str(root),max_runs=limit,max_seconds=10,max_frontier=100,trial_timeout=2)
            with contextlib.redirect_stdout(io.StringIO()):
                code=dfs.explore(args)
            return code,json.loads((root/'run'/'summary.json').read_text())

    def test_irregular_five_leaf_tree_exhausts_and_continues_after_errors(self):
        code,summary=self.exercise(20)
        self.assertEqual(code,0)
        self.assertTrue(summary['exhausted_exposed_prefix_tree'])
        self.assertEqual(summary['unique_runtime_decision_traces'],5)
        self.assertEqual(summary['outcomes'],{'safe':3,'application_bug':2})
        self.assertEqual(summary['pending_prefixes'],0)

    def test_run_limit_keeps_unexplored_prefixes(self):
        code,summary=self.exercise(1)
        self.assertEqual(code,2)
        self.assertFalse(summary['exhausted_exposed_prefix_tree'])
        self.assertEqual(summary['status'],'run_limit')
        self.assertGreater(summary['pending_prefixes'],0)

    def test_fixed_prefix_is_never_branched_again(self):
        ds=[{'step':0,'ordered_bgids':[1,2],'chosen_index':1},
            {'step':1,'ordered_bgids':[3,4],'chosen_index':0}]
        children=list(dfs.branches(ds,1))
        self.assertEqual(len(children),1)
        self.assertEqual(children[0][0],ds[0])
        self.assertEqual(children[0][1]['chosen_index'],1)

    def test_launch_failure_retains_current_prefix(self):
        code,summary=self.exercise(20,executable=False)
        self.assertEqual(code,2)
        self.assertEqual(summary['status'],'incomplete_trial')
        self.assertEqual(summary['pending_prefixes'],1)
        self.assertFalse(summary['exhausted_exposed_prefix_tree'])


if __name__=='__main__':
    unittest.main()
