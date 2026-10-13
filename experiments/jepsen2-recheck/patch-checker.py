#!/usr/bin/env python3
"""Throwaway checker/resource patch. Run AFTER experiments/jepsen/patch-db.py."""
import argparse
import pathlib
import re

parser = argparse.ArgumentParser()
parser.add_argument('upstream', type=pathlib.Path)
parser.add_argument('--algorithm', choices=['wgl', 'competition', 'linear'], default='wgl')
parser.add_argument('--heap', default='10g')
args = parser.parse_args()
if not re.fullmatch(r'[1-9][0-9]*[gm]', args.heap):
    parser.error('heap must be an integer followed by g or m')
p = args.upstream / 'src/jepsen/etcd/register.clj'
s = p.read_text()
old = '''(independent/checker
                  (checker/compose
                    {:linear   (checker/linearizable
                                 {:model (->VersionedRegister 0 nil)})
                     :timeline (timeline/html)}))'''
algorithm = '' if args.algorithm == 'competition' else '\n                                  :algorithm :' + args.algorithm
new = '''(independent/checker
                  (checker/concurrency-limit 1
                    (checker/compose
                      {:linear   (checker/linearizable
                                   {:model (->VersionedRegister 0 nil)ALGORITHM})
                       :timeline (timeline/html)})))'''.replace('ALGORITHM', algorithm)
if s.count(old) != 1:
    raise SystemExit('Expected exact unpatched b47cc401 register checker once; refusing changes')
project = args.upstream / 'project.clj'
t = project.read_text()
if len(re.findall(r'"-Xmx[^"\s]+"', t)) != 1:
    raise SystemExit('Expected exactly one project heap option; refusing changes')
t = re.sub(r'"-Xmx[^"\s]+"', '"-Xmx' + args.heap + '"', t)
p.write_text(s.replace(old, new))
project.write_text(t)
print('Changed only checker scheduling/algorithm and project heap:', args.algorithm, args.heap)
