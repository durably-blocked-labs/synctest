import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
import hashlib
import os
import signal
from unittest import mock

spec = importlib.util.spec_from_file_location("campaign_runner", Path(__file__).with_name("runner.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def task_spec(self, code, seconds=2):
        return {"version": 1, "tasks": [{"id": "one", "independent": True,
                "argv": [sys.executable, "-c", code], "seconds": seconds, "max_bytes": 65536}]}

    def test_zero_exit_without_terminal_stops_and_preserves_resume(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "tasks.json"
            manifest.write_text(json.dumps(self.task_spec("print('no artifact')")))
            directory = root / "run"
            with self.assertRaises(FileNotFoundError):
                runner.run(manifest, directory)
            state = json.loads((directory / "checkpoint.json").read_bytes())
            self.assertEqual(state["tasks"][0]["status"], "failed")
            original = (directory / "task-00000-attempt-00001" / "child.log").read_bytes()
            with self.assertRaises(FileExistsError):
                runner.run(manifest, directory)
            with self.assertRaises(FileNotFoundError):
                runner.run(manifest, directory, True)
            self.assertEqual(original, (directory / "task-00000-attempt-00001" / "child.log").read_bytes())
            self.assertTrue((directory / "task-00000-attempt-00002").is_dir())

    def test_watchdog_and_changed_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "tasks.json"
            manifest.write_text(json.dumps(self.task_spec("import time;time.sleep(5)", 0.05)))
            directory = root / "run"
            with self.assertRaisesRegex(RuntimeError, "watchdog"):
                runner.run(manifest, directory)
            manifest.write_text(json.dumps(self.task_spec("pass")))
            with self.assertRaisesRegex(ValueError, "manifest changed"):
                runner.run(manifest, directory, True)

    def test_parallel_dispatch_and_resume(self):
        code = '''import os,json,time,hashlib
from pathlib import Path
root=Path(os.environ['CAMPAIGN_TASK_DIR'])
(root/'started').write_text(str(time.monotonic()))
time.sleep(.15)
a=root/'artifact';a.mkdir()
m={'version':1,'max_bytes':65536,'max_record_bytes':4096,'workload':'fixture','input_digest':'input','executable_digest':'exe','runtime_identity':'runtime','source_identity':'source','reset_policy':'fresh-process','policy':'delivery-before-time/v1','environment':{'fixture':'true'},'max_decisions':32}
m['task']={'manifest_digest':os.environ['CAMPAIGN_TASK_MANIFEST_DIGEST'],'task_id':os.environ['CAMPAIGN_TASK_ID'],'seed':int(os.environ['CAMPAIGN_TASK_SEED']),'command_digest':os.environ['CAMPAIGN_COMMAND_DIGEST'],'input_digest':os.environ['CAMPAIGN_INPUT_DIGEST']}
m['input_digest']=m['task']['input_digest']
m['seed']=m['task']['seed']
raw=json.dumps(m,separators=(',',':')).encode()+b'\\n'
(a/'manifest.json').write_bytes(raw)
identity=hashlib.sha256(raw).hexdigest()
records=[]
for seq,kind,payload in [(1,'start',{}),(2,'terminal',{'complete':True,'worker_unsafe':False,'replay':{'terminal':'passed'}})]:
 p=json.dumps(payload,separators=(',',':')).encode()
 records.append(json.dumps({'version':1,'sequence':seq,'manifest':identity,'kind':kind,'trial':1,'payload':payload,'checksum':hashlib.sha256(p).hexdigest()},separators=(',',':')))
(a/'events.jsonl').write_text('\\n'.join(records)+'\\n')
(root/'ended').write_text(str(time.monotonic()))
'''
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            spec = self.task_spec(code)
            task = spec['tasks'][0]
            spec['tasks'] = [dict(task, id=str(i), seed=100+i) for i in range(3)]
            manifest = root/'tasks.json'
            manifest.write_text(json.dumps(spec))
            state = runner.run(manifest, root/'run', workers=2)
            self.assertEqual([x['status'] for x in state['tasks']], ['done']*3)
            intervals=[]
            for item in state['tasks']:
                d=root/'run'/item['attempt']
                intervals.append((float((d/'started').read_text()),float((d/'ended').read_text())))
            peak=max(sum(start <= t < end for start,end in intervals) for t,_ in intervals)
            self.assertEqual(peak,2)
            resumed=runner.run(manifest,root/'run',True,2)
            self.assertEqual([x['attempts'] for x in resumed['tasks']],[1,1,1])
            single=runner.run(manifest,root/'single',workers=1,only_task='1')
            self.assertEqual(single['tasks'][1]['seed'],101)
            self.assertEqual([x['status'] for x in single['tasks']],['pending','done','pending'])
            other=root/'run'/state['tasks'][0]['attempt']/'artifact'
            binding=state['tasks'][1]['binding']
            with self.assertRaisesRegex(ValueError,'task binding'):
                runner.validate_terminal(other,['passed'],binding)
            stale=dict(state['tasks'][0]['binding'],manifest_digest='stale')
            with self.assertRaisesRegex(ValueError,'task binding'):
                runner.validate_terminal(other,['passed'],stale)
            # Replace a completed stream by a DIFFERENT self-consistent stream.
            events=other/'events.jsonl'
            rows=[json.loads(line) for line in events.read_text().splitlines()]
            rows[-1]['payload']['new_diagnostic']='replacement'
            import hashlib
            payload=json.dumps(rows[-1]['payload'],separators=(',',':')).encode()
            rows[-1]['checksum']=hashlib.sha256(payload).hexdigest()
            events.write_text(''.join(json.dumps(row,separators=(',',':'))+'\n' for row in rows))
            with self.assertRaisesRegex(ValueError,'completed artifact replaced'):
                runner.run(manifest,root/'run',True,2)

    def test_parallel_failure_cancels_and_joins(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            spec=self.task_spec("import time;time.sleep(.1);raise SystemExit(3)")
            sleeper=dict(spec['tasks'][0],id='two',argv=[sys.executable,'-c',"import os,time;from pathlib import Path;Path(os.environ['CAMPAIGN_TASK_DIR'],'pid').write_text(str(os.getpid()));time.sleep(5)"])
            spec['tasks'].append(sleeper)
            spec['tasks'].append(dict(sleeper,id='never'))
            manifest=root/'tasks.json';manifest.write_text(json.dumps(spec))
            with self.assertRaisesRegex(RuntimeError,'child exit'):
                runner.run(manifest,root/'run',workers=2)
            state=json.loads((root/'run'/'checkpoint.json').read_bytes())
            self.assertEqual([x['status'] for x in state['tasks']],['failed','cancelled','pending'])
            import os
            pid=int((root/'run'/state['tasks'][1]['attempt']/'pid').read_text())
            with self.assertRaises(ProcessLookupError): os.kill(pid,0)

    def artifact(self, root, terminals, trial=1, start=True, seed=42):
        binding = {'manifest_digest':'manifest','task_id':'one','seed':42,'command_digest':'command','input_digest':'input'}
        manifest = dict(version=1, workload='fixture', input_digest='input', executable_digest='exe', runtime_identity='runtime', source_identity='source', reset_policy='fresh', policy='policy', environment={'fixture':'true'}, max_decisions=32, max_bytes=65536, max_record_bytes=4096, task=binding, seed=seed)
        raw=runner.canonical(manifest)+b'\n'
        (root/'manifest.json').write_bytes(raw)
        records=[]
        for terminal in terminals:
            for kind,payload in ([('start',{})] if start else []) + [('terminal',terminal)]:
                records.append(dict(version=1, sequence=len(records)+1, manifest=hashlib.sha256(raw).hexdigest(), kind=kind, trial=trial, payload=payload, checksum=hashlib.sha256(runner.canonical(payload)).hexdigest()))
            if type(trial) is int: trial += 1
        (root/'events.jsonl').write_bytes(b''.join(runner.canonical(row)+b'\n' for row in records))
        return binding

    def test_every_terminal_seed_and_active_trial_validated(self):
        good=dict(complete=True, worker_unsafe=False, replay={'terminal':'passed'})
        for bad in [dict(good,worker_unsafe=True),dict(good,complete=False),dict(good,replay={'terminal':'resource-limit'}),dict(good,replay={'terminal':'infrastructure-failed'})]:
            with self.subTest(bad=bad), tempfile.TemporaryDirectory() as temp:
                root=Path(temp); binding=self.artifact(root,[bad,good])
                with self.assertRaises(ValueError): runner.validate_terminal(root,['passed'],binding)
        for trial in [None,0,-1,True]:
            with self.subTest(trial=trial), tempfile.TemporaryDirectory() as temp:
                root=Path(temp); binding=self.artifact(root,[good],trial=trial,start=False)
                with self.assertRaises(ValueError): runner.validate_terminal(root,['passed'],binding)
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); binding=self.artifact(root,[good],seed=7)
            with self.assertRaisesRegex(ValueError,'task binding'): runner.validate_terminal(root,['passed'],binding)

    def test_checkpoint_unique_temp_and_interrupted_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'checkpoint.json'
            path.with_suffix('.json.tmp').write_text('stale')
            runner.checkpoint(path,{'version':1})
            with mock.patch.object(runner.os,'replace',side_effect=InterruptedError('interrupted')):
                with self.assertRaises(InterruptedError): runner.checkpoint(path,{'version':2})
            self.assertEqual(json.loads(path.read_text()),{'version':1})
            self.assertEqual(len(list(path.parent.glob('checkpoint.json.*.tmp'))),0)
            runner.checkpoint(path,{'version':3})

    def test_launch_failure_durable_and_resume_metadata_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); manifest=root/'tasks.json'; directory=root/'run'
            spec=self.task_spec('pass'); spec['tasks'][0]['argv']=['/nonexistent/bubbles-test-command']
            manifest.write_text(json.dumps(spec))
            with self.assertRaises(FileNotFoundError): runner.run(manifest,directory)
            state=json.loads((directory/'checkpoint.json').read_text())
            self.assertEqual(state['tasks'][0]['status'],'failed')
            self.assertIn('nonexistent',state['tasks'][0]['error'])
            self.assertFalse((directory/'runner.lock').exists())
            for key,value in [('id','other'),('seed',999),('expected',['diverged']),('binding',{})]:
                changed=json.loads(json.dumps(state)); changed['tasks'][0][key]=value
                runner.checkpoint(directory/'checkpoint.json',changed)
                with self.assertRaisesRegex(ValueError,'task metadata'): runner.run(manifest,directory,True)
            changed=json.loads(json.dumps(state)); changed['tasks']=[]
            runner.checkpoint(directory/'checkpoint.json',changed)
            with self.assertRaisesRegex(ValueError,'task metadata'): runner.run(manifest,directory,True)

    def test_startup_log_failure_and_resume_task_reorder(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); manifest=root/'tasks.json'; directory=root/'run'
            spec=self.task_spec('pass'); spec['tasks'].append(dict(spec['tasks'][0],id='two'))
            manifest.write_text(json.dumps(spec))
            original=Path.open
            def opened(path,*args,**kwargs):
                if path.name=='child.log': raise OSError('log startup failed')
                return original(path,*args,**kwargs)
            with mock.patch.object(Path,'open',opened):
                with self.assertRaisesRegex(OSError,'log startup failed'): runner.run(manifest,directory)
            state=json.loads((directory/'checkpoint.json').read_text())
            self.assertEqual(state['tasks'][0]['status'],'failed')
            self.assertEqual(state['tasks'][0]['error'],'log startup failed')
            state['tasks'].reverse(); runner.checkpoint(directory/'checkpoint.json',state)
            with self.assertRaisesRegex(ValueError,'task metadata'): runner.run(manifest,directory,True)

    def test_repeated_cancellation_during_cleanup_is_contained(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); manifest=root/'tasks.json'; directory=root/'run'
            manifest.write_text(json.dumps(self.task_spec('import time;time.sleep(30)',.01)))
            original=runner.subprocess.Popen; cleanup=runner.terminate_all; spawned=[]
            def start(*args,**kwargs):
                process=original(*args,**kwargs); spawned.append(process); return process
            def interrupted_cleanup(processes):
                os.kill(os.getpid(),signal.SIGTERM)
                cleanup(processes)
            old=signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(InterruptedError('repeat cancel')))
            try:
                with mock.patch.object(runner.subprocess,'Popen',side_effect=start), mock.patch.object(runner,'terminate_all',side_effect=interrupted_cleanup):
                    with self.assertRaises(InterruptedError): runner.run(manifest,directory)
            finally:
                signal.signal(signal.SIGTERM,old)
                for process in spawned: runner.terminate(process)
            self.assertIsNotNone(spawned[0].poll())
            self.assertFalse((directory/'runner.lock').exists())
            self.assertEqual(json.loads((directory/'checkpoint.json').read_text())['tasks'][0]['status'],'failed')

    def test_cancellation_after_spawn_is_contained(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); manifest=root/'tasks.json'; directory=root/'run'
            manifest.write_text(json.dumps(self.task_spec('import time;time.sleep(30)')))
            original=runner.subprocess.Popen; spawned=[]
            def start(*args,**kwargs):
                process=original(*args,**kwargs); spawned.append(process)
                os.kill(os.getpid(),signal.SIGTERM)
                return process
            old=signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(InterruptedError('cancel fixture')))
            try:
                with mock.patch.object(runner.subprocess,'Popen',side_effect=start):
                    with self.assertRaises(InterruptedError): runner.run(manifest,directory)
            finally:
                signal.signal(signal.SIGTERM,old)
            self.assertIsNotNone(spawned[0].poll())
            self.assertFalse((directory/'runner.lock').exists())
            state=json.loads((directory/'checkpoint.json').read_text())
            self.assertEqual(state['tasks'][0]['status'],'failed')

    def test_containment_failure_retains_lease(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); manifest=root/'tasks.json'; directory=root/'run'
            manifest.write_text(json.dumps(self.task_spec('import time;time.sleep(30)',.01)))
            original=runner.subprocess.Popen; spawned=[]
            def start(*args,**kwargs):
                process=original(*args,**kwargs); spawned.append(process); return process
            try:
                with mock.patch.object(runner.subprocess,'Popen',side_effect=start), mock.patch.object(runner,'terminate_all',side_effect=RuntimeError('containment failed')):
                    with self.assertRaisesRegex(RuntimeError,'containment failed'): runner.run(manifest,directory)
                self.assertTrue((directory/'runner.lock').exists())
            finally:
                for process in spawned: runner.terminate(process)

    def test_empty_tasks_rejected_before_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "tasks.json"
            manifest.write_text('{"version":1,"tasks":[]}')
            with self.assertRaises(ValueError):
                runner.run(manifest, root / "run")
            self.assertFalse((root / "run").exists())


if __name__ == "__main__":
    unittest.main()
