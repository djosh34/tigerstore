#!/usr/bin/env python3
"""Reconstruct observed HTTP/2 connection credits from complete captures.

This uses TLS Read/Write completion ordering, not packet capture timestamps.
It records advertised credit, not whether a remote writer had queued DATA.
"""
from collections import defaultdict
import gzip
import json
from pathlib import Path

for path in sorted(Path('evidence/pre47batch256/diagnose').glob('c*/*/n*-trace.json.gz')):
    data=json.load(gzip.open(path,'rt'))
    assert not data['dropped']
    events=sorted(data['events'],key=lambda e:e['ns'])
    credits=defaultdict(lambda:dict(request=65535,response=65535,min_request=65535,min_response=65535,request_bytes=0,response_bytes=0,request_updates=0,response_updates=0))
    connections=set()
    settings=defaultdict(set)
    for e in events:
        k=e['k']; conn=e.get('a',0); stream=e.get('b',0); value=e.get('c',0)
        if k=='h2_conn_port': connections.add(conn)
        if k.startswith('h2_') and 'settings' in k: settings[k].add(value)
        direction=None
        if k=='h2_in_data': direction='request'; change=-value; credits[conn]['request_bytes']+=value
        elif k=='h2_out_data': direction='response'; change=-value; credits[conn]['response_bytes']+=value
        elif k=='h2_in_window_update' and stream==0: direction='response'; change=value; credits[conn]['response_updates']+=1
        elif k=='h2_out_window_update' and stream==0: direction='request'; change=value; credits[conn]['request_updates']+=1
        if direction:
            assert conn in connections, 'capture started after connection setup'
            c=credits[conn]
            c[direction]+=change
            c['min_'+direction]=min(c['min_'+direction],c[direction])
    result={'connections':len(connections),'settings':{k:sorted(v) for k,v in settings.items()},
            'min_request_credit':min((c['min_request'] for c in credits.values()),default=65535),
            'min_response_credit':min((c['min_response'] for c in credits.values()),default=65535),
            'per_connection':dict(credits)}
    out=path.with_name(path.name.replace('-trace.json.gz','-h2-summary.json'))
    out.write_text(json.dumps(result,indent=2)+'\n')
    print(path, result['connections'],result['settings'],result['min_request_credit'],result['min_response_credit'])
