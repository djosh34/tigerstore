#!/usr/bin/env python3
"""Generate an explicit benchmark-only Go overlay, never edit production files.

The sole KV backend seam is concrete node.Node.SubmitBatch. This overlay returns
fixtures immediately at that seam, retaining the production server framing,
admission, alias decoding, completion encoding, accounting and metrics.
Watch injection is at the node's outbound WatchStream.C, not a replacement gRPC
server. It retains production watch encoding, HTTP/2, TLS and clientv3 delivery.
"""
import json
from pathlib import Path

root = Path.cwd()
out = root / 'research/485/stack/generated'
out.mkdir(exist_ok=True)
src = root / 'internal/node/node.go'
s = src.read_text()
needle = 'func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {'
assert s.count(needle) == 1
start = s.index(needle)
end = s.index('\n}\n', start) + 2
s = s[:start] + '''// EXP485: immediate-answer fixtures at the concrete KV backend seam.
var exp485Key = []byte("key")
var exp485Value = make([]byte, 1024)
var Exp485Watch = make(chan *WatchStream, 1)

func (ws *WatchStream) Exp485Deliver(r *pb.WatchResponse) {
 ws.queued.Add(int64(proto.Size(r)))
 ws.C <- r
}

func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {
 cs := make([]Completion, len(reqs))
 for i, req := range reqs {
  r := core.Reply{Rev:3, Term:1}
  switch req.Op.(type) {
  case *pb.RangeRequest:
   r.Range = mvcc.RangeResult{Rev:3, Count:1, Bytes:1027, KVs:[]mvcc.KV{{Key:exp485Key, Value:exp485Value, CreateRevision:2, ModRevision:3, Version:2}}}
  case *pb.TxnRequest:
   r.Resp = &pb.TxnResponse{Header:&pb.ResponseHeader{ClusterId:n.ClusterID(), MemberId:n.ID(), Revision:3, RaftTerm:1}, Succeeded:true, Responses:[]*pb.ResponseOp{{Response:&pb.ResponseOp_ResponsePut{ResponsePut:&pb.PutResponse{}}}}}
  default:
   // The production server admits Put as core.Put (Op is nil).
  }
  cs[i] = Completion{Tag:tags[i], Reply:r}
 }
 to.Complete(cs)
 return nil
}''' + s[end:]
needle = '\n\treturn ws, nil\n}'
assert s.count(needle) == 1
s = s.replace(needle, '\n\tExp485Watch <- ws\n\treturn ws, nil\n}')
dest = out / 'node.go'
dest.write_text(s)
(root / 'research/485/stack/overlay.json').write_text(json.dumps({'Replace': {str(src):str(dest)}}))
