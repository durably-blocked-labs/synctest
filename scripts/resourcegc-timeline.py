#!/usr/bin/env python3
"""Render the explicitly correlated observation window from a saved trial."""
import argparse
import json
from pathlib import Path


def render(result):
    timeline=result.get('timeline',[])
    if result.get('observation_overflow'):
        raise ValueError('timeline snapshot is incomplete')
    roles={}
    for event in timeline:
        if event.get('call'):
            role=event['call']['actor']
            old=roles.setdefault(event['bgid'],role)
            if old!=role:raise ValueError('actor identity changed')
        elif event.get('tag') in (1001,1002):
            roles[event['bgid']]='probe actor'
    lines=['# Correlated execution timeline','',
           'Sequence orders event reservations. GC resume-request records precede actual resumption.',
           'Call source locations identify function definitions; GC stack locations identify captured frames.',
           '', '| Sequence | Event | Actor | Scheduler step | Source |',
           '| ---: | --- | --- | ---: | --- |']
    stacks={}
    for index,event in enumerate(timeline,1):
        if event['sequence']!=index:raise ValueError('noncontiguous observation sequence')
        actor=(roles.get(event['bgid'],'harness/other')+f" (B{event['bgid']})"
               if event['bgid'] else 'unassigned runtime helper')
        kind=event['kind']
        source=event.get('captured_site') or {}
        label=kind
        if kind=='call' and event.get('call'):
            call=event['call']
            label=call['operation']+' '+call['phase']
            source=call
            stack=stacks.setdefault(event['bgid'],[])
            if call['phase']=='enter':stack.append(call['operation'])
            elif not stack or stack.pop()!=call['operation']:
                raise ValueError('unbalanced call boundaries')
        elif kind=='call':
            label={1001:'before channel wait',1002:'after channel release'}.get(event.get('tag'),'marker '+str(event.get('tag')))
        elif kind=='gc_scan_begin':
            label=f"GC scan; stopped={str(event['stopped']).lower()}"
            source=next((f for f in event.get('stack',[]) if '/experiments/resourcegc.' in f.get('function','')),source)
        elif kind=='gc_resume_requested':label='GC resume requested'
        elif kind=='preempt':
            label=f"preempt; STW={event.get('stw_reason',0)}; synchronous={event.get('sync_safe_point',False)}"
            source=next((f for f in event.get('stack',[]) if '/experiments/resourcegc.' in f.get('function','')),source)
        elif kind=='preempt_request':label=f"request preemption; STW={event.get('stw_reason',0)}"
        elif kind=='world_stop_wait':label=f"stop P; STW={event.get('stw_reason',0)}"
        elif kind=='world_stop_resumed':label=f"resume P; STW={event.get('stw_reason',0)}"
        elif kind=='world_stop_continued':label=f"continue interrupted actor; no decision; STW={event.get('stw_reason',0)}"
        elif kind=='schedule':label='schedule actor'
        location=f"{Path(source['file']).name}:{source['line']}" if source.get('file') else '—'
        lines.append(f"| {index} | {label} | {actor} | {event['native_step']} | {location} |")
    if any(stacks.values()):raise ValueError('unfinished call boundaries in completed application window')
    lines += ['', 'Full raw scheduler trace and captured stacks remain in the trial JSON.', '']
    return '\n'.join(lines)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('result')
    parser.add_argument('--output')
    args=parser.parse_args()
    text=render(json.loads(Path(args.result).read_text()))
    if args.output:
        with Path(args.output).open('x') as stream:stream.write(text)
    else:print(text)
