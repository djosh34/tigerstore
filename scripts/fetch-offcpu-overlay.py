"""Diagnostic-only, identical sampling/cause observers on both frozen runtimes."""
from pathlib import Path
import subprocess

CAUSES = ('held_output_share', 'partial_input', 'side_input', 'partial_timeout',
          'ring_checkpoint_pending', 'ring_room', 'forwarded_refusal', 'busy_total')
PREFIX = 'tigerstore_offcpu_'
GAUGES = ('ring_room_bytes', 'ring_taken_bytes', 'ring_short', 'apply_gap',
          'held_replies', 'room_waiters')


def replace(text, old, new, count=1):
    if text.count(old) != count:
        raise ValueError(f'diagnostic anchor count {text.count(old)} != {count}: {old}')
    return text.replace(old, new)


def prepare(checkout, evidence, side):
    directory = checkout / 'internal/wl'
    for filename, indexes in (('node.go', [0]), ('h2.go', [1]), ('side.go', [2]),
                              ('partwait.go', [3]), ('logring.go', [4, 5])):
        p = directory / filename
        text = p.read_text()
        anchor = 'n.reject(c, s, errTooManyRequests)'
        if text.count(anchor) != len(indexes):
            raise ValueError(f'{filename}: refusal sites changed')
        parts = text.split(anchor)
        text = parts[0]
        for index, suffix in zip(indexes, parts[1:]):
            text += f'fetchOffcpuCause(s, {index})\n' + anchor + suffix
        if filename == 'node.go':
            text = replace(text, 'func (n *node) reject(c *client, s *stream, err error) {',
                           'func (n *node) reject(c *client, s *stream, err error) {\n'
                           '\tif err == errTooManyRequests { fetchOffcpuCause(s, 7) }')
        p.write_text(text)
    p = directory / 'refusal.go'
    text = p.read_text()
    text = replace(text, '\t\t\tn.reject(c, s, err)',
                   '\t\t\tif err == errTooManyRequests { fetchOffcpuCause(s, 6) }\n\t\t\tn.reject(c, s, err)')
    p.write_text(text)
    names = list(CAUSES) + list(GAUGES)
    descriptors = ',\n'.join(f'prometheus.NewDesc("{PREFIX}{name}", "Diagnostic-only txn refusal cause or loop state.", nil, nil)' for name in names)
    helper = directory / 'fetch_offcpu_diagnostic.go'
    helper.write_text('''package wl
import (
 "runtime"
 "github.com/prometheus/client_golang/prometheus"
)
// Diagnostic checkout only. No counters or sampling rates enter production.
func init() { runtime.SetBlockProfileRate(1000); runtime.SetMutexProfileFraction(1) }
var fetchOffcpuRefusals [8]uint64
func fetchOffcpuCause(s *stream, cause int) {
 if s.kind == kTxn { fetchOffcpuRefusals[cause]++ }
}
var fetchOffcpuDescs = []*prometheus.Desc{''' + descriptors + ''',
}
func fetchOffcpuNode(n *node) [14]float64 {
 var out [14]float64
 for i, v := range fetchOffcpuRefusals { out[i] = float64(v) }
 if n.wal != nil && n.wal.ring != nil {
  out[8] = float64(n.wal.ringRoom())
  out[9] = float64(n.ringTaken())
  if n.wal.ringShort() { out[10] = 1 }
 }
 out[11] = float64(n.commit - min(n.commit, n.applied))
 out[12] = float64(len(n.heldReplies))
 out[13] = float64(len(n.roomWait))
 return out
}
''')
    p = directory / 'prom.go'
    text = p.read_text()
    text = replace(text, 'type promSnap struct {\n', 'type promSnap struct {\n\tfetchOffcpu [14]float64\n')
    text = replace(text, 'p := &promSnap{stats: n.gs, format: n.format}',
                   'p := &promSnap{stats: n.gs, format: n.format}\n\tp.fetchOffcpu = fetchOffcpuNode(n)')
    text = replace(text, 'func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {\n',
                   'func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {\n\tfor _, d := range fetchOffcpuDescs { ch <- d }\n')
    anchor = '\tif p == nil {\n\t\treturn\n\t}'
    text = replace(text, anchor, anchor + '\n\tfor i, d := range fetchOffcpuDescs { constGauge(ch, d, p.fetchOffcpu[i]) }')
    p.write_text(text)
    files = [directory / name for name in ('node.go', 'h2.go', 'side.go', 'partwait.go', 'logring.go', 'refusal.go', 'prom.go', helper.name)]
    subprocess.run(['gofmt', '-w', *map(str, files)], check=True)
    dest = evidence / f'{side}-offcpu-overlay'
    dest.mkdir()
    for p in files:
        (dest / p.name).write_bytes(p.read_bytes())
    (dest / 'tracked.diff').write_bytes(subprocess.check_output(['git', 'diff', '--binary'], cwd=checkout))
