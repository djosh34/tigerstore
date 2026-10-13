# Verified sites — exact source text

Etcd source bound to `68c065e562994b89e333e77b039ad066f933c586`. Dependency directory names bind the module version; module ZIP h1 and stock buildinfo are checked in `verification.json` and dependency manifests. Numbered text below is unmodified source line content (only line-number prefixes are added). Each reported range is checked against archive/ZIP bytes.

## `deps/github.com_golang_protobuf@v1.5.4/proto/wire.go:22-40`

SHA256: `84e9dff5c3cf0eddf5f5050c61c94d863cea3f7b2ae81270d0790959f0ca2581`; binding: `github.com_golang_protobuf@v1.5.4`

```text
22: func Marshal(m Message) ([]byte, error) {
23: 	b, err := marshalAppend(nil, m, false)
24: 	if b == nil {
25: 		b = zeroBytes
26: 	}
27: 	return b, err
28: }
29: 
30: var zeroBytes = make([]byte, 0, 0)
31: 
32: func marshalAppend(buf []byte, m Message, deterministic bool) ([]byte, error) {
33: 	if m == nil {
34: 		return nil, ErrNil
35: 	}
36: 	mi := MessageV2(m)
37: 	nbuf, err := protoV2.MarshalOptions{
38: 		Deterministic: deterministic,
39: 		AllowPartial:  true,
40: 	}.MarshalAppend(buf, mi)
```

## `deps/github.com_golang_protobuf@v1.5.4/proto/wire.go:56-69`

SHA256: `84e9dff5c3cf0eddf5f5050c61c94d863cea3f7b2ae81270d0790959f0ca2581`; binding: `github.com_golang_protobuf@v1.5.4`

```text
56: func Unmarshal(b []byte, m Message) error {
57: 	m.Reset()
58: 	return UnmarshalMerge(b, m)
59: }
60: 
61: // UnmarshalMerge parses a wire-format message in b and places the decoded results in m.
62: func UnmarshalMerge(b []byte, m Message) error {
63: 	mi := MessageV2(m)
64: 	out, err := protoV2.UnmarshalOptions{
65: 		AllowPartial: true,
66: 		Merge:        true,
67: 	}.UnmarshalState(protoiface.UnmarshalInput{
68: 		Buf:     b,
69: 		Message: mi.ProtoReflect(),
```

## `deps/go.etcd.io_bbolt@v1.5.0/bucket.go:476-492`

SHA256: `40124715d9e7192225dad52d3cbe4d9547af1388f63c4a40a4e3dc9a57f4919a`; binding: `go.etcd.io_bbolt@v1.5.0`

```text
476: 	// Tip: Use a new variable `newKey` instead of reusing the existing `key` to prevent
477: 	// it from being marked as leaking, and accordingly cannot be allocated on stack.
478: 	newKey := cloneBytes(key)
479: 
480: 	// Move cursor to correct position.
481: 	c := b.Cursor()
482: 	k, _, flags := c.seek(newKey)
483: 
484: 	// Return an error if there is an existing key with a bucket value.
485: 	if bytes.Equal(newKey, k) && (flags&common.BucketLeafFlag) != 0 {
486: 		return errors.ErrIncompatibleValue
487: 	}
488: 
489: 	// gofail: var beforeBucketPut struct{}
490: 
491: 	c.node().put(newKey, newKey, value, 0, 0)
492: 
```

## `deps/go.etcd.io_bbolt@v1.5.0/internal/common/inode.go:100-101`

SHA256: `dab0a28fcf562dd7a1e917e6e20b177de320267f36990a31a8362c4878d5bfd2`; binding: `go.etcd.io_bbolt@v1.5.0`

```text
100: 		l := copy(b, item.Key())
101: 		copy(b[l:], item.Value())
```

## `deps/go.etcd.io_bbolt@v1.5.0/node.go:199`

SHA256: `99d4adf5b0dc2e57219bac3681913054ac8d2ccc2052b12e7027b2aca27ee309`; binding: `go.etcd.io_bbolt@v1.5.0`

```text
199: 	common.WriteInodeToPage(n.inodes, p)
```

## `deps/go.etcd.io_bbolt@v1.5.0/node.go:334`

SHA256: `99d4adf5b0dc2e57219bac3681913054ac8d2ccc2052b12e7027b2aca27ee309`; binding: `go.etcd.io_bbolt@v1.5.0`

```text
334: 		node.write(p)
```

## `deps/go.etcd.io_bbolt@v1.5.0/node.go:478-481`

SHA256: `99d4adf5b0dc2e57219bac3681913054ac8d2ccc2052b12e7027b2aca27ee309`; binding: `go.etcd.io_bbolt@v1.5.0`

```text
478: 
479: 		value := make([]byte, len(inode.Value()))
480: 		copy(value, inode.Value())
481: 		inode.SetValue(value)
```

## `deps/go.etcd.io_raft_v3@v3.7.0/log_unstable.go:200-218`

SHA256: `9cdbd352eefb755342819adb7503f99f41a82259181e0ab929c801947179f3d0`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
200: func (u *unstable) truncateAndAppend(ents []*pb.Entry) {
201: 	fromIndex := ents[0].GetIndex()
202: 	switch {
203: 	case fromIndex == u.offset+uint64(len(u.entries)):
204: 		// fromIndex is the next index in the u.entries, so append directly.
205: 		u.entries = append(u.entries, ents...)
206: 	case fromIndex <= u.offset:
207: 		u.logger.Infof("replace the unstable entries from index %d", fromIndex)
208: 		// The log is being truncated to before our current offset
209: 		// portion, so set the offset and replace the entries.
210: 		u.entries = ents
211: 		u.offset = fromIndex
212: 		u.offsetInProgress = u.offset
213: 	default:
214: 		// Truncate to fromIndex (exclusive), and append the new entries.
215: 		u.logger.Infof("truncate the unstable entries before index %d", fromIndex)
216: 		keep := u.slice(u.offset, fromIndex) // NB: appending to this slice is safe,
217: 		u.entries = append(keep, ents...)    // and will reallocate/copy it
218: 		// Only in-progress entries before fromIndex are still considered to be
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:386-389`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
386: 		case pm := <-propc:
387: 			m := pm.m
388: 			m.From = new(r.id)
389: 			err := r.Step(m)
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:391-393`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
391: 				pm.result <- err
392: 				close(pm.result)
393: 			}
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:395`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
395: 			if IsResponseMsg(m.GetType()) && !IsLocalMsgTarget(m.GetFrom()) && r.trk.Progress[m.GetFrom()] == nil {
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:433`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
433: 		case <-n.tickc:
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:435`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
435: 		case readyc <- rd:
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:443`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
443: 		case <-advancec:
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:460`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
460: 	case n.tickc <- struct{}{}:
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:471-473`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
471: func (n *node) Propose(ctx context.Context, data []byte) error {
472: 	return n.stepWait(ctx, &pb.Message{Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Data: data}}})
473: }
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:508-510`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
508: func (n *node) stepWait(ctx context.Context, m *pb.Message) error {
509: 	return n.stepWithWaitOption(ctx, m, true)
510: }
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:517`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
517: 		case n.recvc <- m:
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:528`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
528: 		pm.result = make(chan error, 1)
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:531-534`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
531: 	case ch <- pm:
532: 		if !wait {
533: 			return nil
534: 		}
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:541`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
541: 	case err := <-pm.result:
```

## `deps/go.etcd.io_raft_v3@v3.7.0/node.go:557`

SHA256: `311ece789019299836c97ca24908a52ca0e96ea11d3c81109444b41f625607a9`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
557: 	case n.advancec <- struct{}{}:
```

## `deps/go.etcd.io_raft_v3@v3.7.0/raft.go:812-820`

SHA256: `d3d8fa573e1488e3aa35b9b997ba943454f3f357740a550d4bcc44d81975f07f`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
812: func (r *raft) appendEntry(es ...*pb.Entry) (accepted bool) {
813: 	li := r.raftLog.lastIndex()
814: 	cloned := make([]*pb.Entry, len(es))
815: 	for i := range es {
816: 		// TODO: Consider whether cloning/copying is necessary at all, and aim to eliminate it if possible.
817: 		cloned[i] = proto.Clone(es[i]).(*pb.Entry)
818: 		cloned[i].Term = new(r.Term)
819: 		cloned[i].Index = new(li + 1 + uint64(i))
820: 	}
```

## `deps/go.etcd.io_raft_v3@v3.7.0/raftpb/raft.pb.go:342-348`

SHA256: `d94c220250d54147f849f7c416787be4971903b4c95d93570e7906041513ff99`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
342: 
343: type Entry struct {
344: 	state         protoimpl.MessageState `protogen:"open.v1"`
345: 	Term          *uint64                `protobuf:"varint,2,opt,name=Term" json:"Term,omitempty"`
346: 	Index         *uint64                `protobuf:"varint,3,opt,name=Index" json:"Index,omitempty"`
347: 	Type          *EntryType             `protobuf:"varint,1,opt,name=Type,enum=raftpb.EntryType" json:"Type,omitempty"`
348: 	Data          []byte                 `protobuf:"bytes,4,opt,name=Data" json:"Data,omitempty"`
```

## `deps/go.etcd.io_raft_v3@v3.7.0/storage.go:293-320`

SHA256: `22020183114fe7555bc44ebda1b5b0d67de6534b0c8735c08c8667a0bd245403`; binding: `go.etcd.io_raft_v3@v3.7.0`

```text
293: func (ms *MemoryStorage) Append(entries []*pb.Entry) error {
294: 	if len(entries) == 0 {
295: 		return nil
296: 	}
297: 
298: 	ms.Lock()
299: 	defer ms.Unlock()
300: 
301: 	first := ms.firstIndex()
302: 	last := entries[0].GetIndex() + uint64(len(entries)) - 1
303: 
304: 	// shortcut if there is no new entry.
305: 	if last < first {
306: 		return nil
307: 	}
308: 	// truncate compacted entries
309: 	if first > entries[0].GetIndex() {
310: 		entries = entries[first-entries[0].GetIndex():]
311: 	}
312: 
313: 	offset := entries[0].GetIndex() - ms.ents[0].GetIndex()
314: 	switch {
315: 	case uint64(len(ms.ents)) > offset:
316: 		// NB: full slice expression protects ms.ents at index >= offset from
317: 		// rewrites, as they may still be referenced from outside MemoryStorage.
318: 		ms.ents = append(ms.ents[:offset:offset], entries...)
319: 	case uint64(len(ms.ents)) == offset:
320: 		ms.ents = append(ms.ents, entries...)
```

## `deps/google.golang.org_grpc@v1.83.2/encoding/proto/proto.go:64-78`

SHA256: `e49a349919237058e91aa19cf2128f78fdbdaae0f8ed169a600ffcc07ff566bd`; binding: `google.golang.org_grpc@v1.83.2`

```text
64: 	marshalOptions := proto.MarshalOptions{UseCachedSize: true}
65: 
66: 	if mem.IsBelowBufferPoolingThreshold(size) {
67: 		buf, err := marshalOptions.Marshal(vv)
68: 		if err != nil {
69: 			return nil, err
70: 		}
71: 		data = append(data, mem.SliceBuffer(buf))
72: 	} else {
73: 		pool := mem.DefaultBufferPool()
74: 		buf := pool.Get(size)
75: 		if _, err := marshalOptions.MarshalAppend((*buf)[:0], vv); err != nil {
76: 			pool.Put(buf)
77: 			return nil, err
78: 		}
```

## `deps/google.golang.org_grpc@v1.83.2/encoding/proto/proto.go:85-96`

SHA256: `e49a349919237058e91aa19cf2128f78fdbdaae0f8ed169a600ffcc07ff566bd`; binding: `google.golang.org_grpc@v1.83.2`

```text
85: func (c *codecV2) Unmarshal(data mem.BufferSlice, v any) (err error) {
86: 	vv := messageV2Of(v)
87: 	if vv == nil {
88: 		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
89: 	}
90: 
91: 	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
92: 	defer buf.Free()
93: 	// TODO: Upgrade proto.Unmarshal to support mem.BufferSlice. Right now, it's not
94: 	//  really possible without a major overhaul of the proto package, but the
95: 	//  vtprotobuf library may be able to support this.
96: 	return proto.Unmarshal(buf.ReadOnlyData(), vv)
```

## `deps/google.golang.org_grpc@v1.83.2/internal/transport/http2_server.go:603-615`

SHA256: `84ad3bde6a86be6ab86af02adc250ae04418fb24aafa628292141fb6fc9ed093`; binding: `google.golang.org_grpc@v1.83.2`

```text
603: 		// We need to wait for s.cancel to be updated before calling
604: 		// t.closeStream to avoid data races.
605: 		cancelUpdated := make(chan struct{})
606: 		timer := internal.TimeAfterFunc(timeout, func() {
607: 			<-cancelUpdated
608: 			t.closeStream(s, true, http2.ErrCodeCancel, false)
609: 		})
610: 		oldCancel := s.cancel
611: 		s.cancel = func() {
612: 			oldCancel()
613: 			timer.Stop()
614: 		}
615: 		close(cancelUpdated)
```

## `deps/google.golang.org_grpc@v1.83.2/mem/buffer_slice.go:79-83`

SHA256: `eb963947548f5a64d07df1f79d06746adfdb597cef66e6b4207ff4389edd8340`; binding: `google.golang.org_grpc@v1.83.2`

```text
79: func (s BufferSlice) CopyTo(dst []byte) int {
80: 	off := 0
81: 	for _, b := range s {
82: 		off += copy(dst[off:], b.ReadOnlyData())
83: 	}
```

## `deps/google.golang.org_grpc@v1.83.2/mem/buffer_slice.go:105-117`

SHA256: `eb963947548f5a64d07df1f79d06746adfdb597cef66e6b4207ff4389edd8340`; binding: `google.golang.org_grpc@v1.83.2`

```text
105: func (s BufferSlice) MaterializeToBuffer(pool BufferPool) Buffer {
106: 	if len(s) == 1 {
107: 		s[0].Ref()
108: 		return s[0]
109: 	}
110: 	sLen := s.Len()
111: 	if sLen == 0 {
112: 		return emptyBuffer{}
113: 	}
114: 	buf := pool.Get(sLen)
115: 	s.CopyTo(*buf)
116: 	return NewBuffer(buf, pool)
117: }
```

## `deps/google.golang.org_grpc@v1.83.2/server.go:1187-1189`

SHA256: `a09f192cb707172d7b9004813f300697e0eab8eb781389978683460ed0ddc49e`; binding: `google.golang.org_grpc@v1.83.2`

```text
1187: 
1188: func (s *Server) sendResponse(ctx context.Context, stream *transport.ServerStream, msg any, cp Compressor, opts *transport.WriteOptions, comp encoding.Compressor) error {
1189: 	data, err := encode(s.getCodec(stream.ContentSubtype()), msg)
```

## `deps/google.golang.org_grpc@v1.84.0/internal/transport/http2_client.go:618-626`

SHA256: `93fc9536fa02161c8559c21da7e26359d06d7ae4368a4ee7a4a6661cfe094491`; binding: `google.golang.org_grpc@v1.84.0`

```text
618: 	}
619: 	if dl, ok := ctx.Deadline(); ok {
620: 		// Send out timeout regardless its value. The server can detect timeout context by itself.
621: 		// TODO(mmukhi): Perhaps this field should be updated when actually writing out to the wire.
622: 		timeout := time.Until(dl)
623: 		if timeout <= 0 {
624: 			return nil, status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())
625: 		}
626: 		headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-timeout", Value: grpcutil.EncodeDuration(timeout)})
```

## `deps/google.golang.org_protobuf@v1.36.11/encoding/protowire/wire.go:454-456`

SHA256: `109e891e95fb059a3622e2ad97bb06f6829da7d4ac7fc13c36deb508d6f9ef40`; binding: `google.golang.org_protobuf@v1.36.11`

```text
454: func AppendBytes(b []byte, v []byte) []byte {
455: 	return append(AppendVarint(b, uint64(len(v))), v...)
456: }
```

## `deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_gen.go:5402-5406`

SHA256: `4eed98966d930db1855cadf0972d255cd89733e29815ef4d3904e3fe0e473a25`; binding: `google.golang.org_protobuf@v1.36.11`

```text
5402: func appendBytes(b []byte, p pointer, f *coderFieldInfo, opts marshalOptions) ([]byte, error) {
5403: 	v := *p.Bytes()
5404: 	b = protowire.AppendVarint(b, f.wiretag)
5405: 	b = protowire.AppendBytes(b, v)
5406: 	return b, nil
```

## `deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_gen.go:5477-5484`

SHA256: `4eed98966d930db1855cadf0972d255cd89733e29815ef4d3904e3fe0e473a25`; binding: `google.golang.org_protobuf@v1.36.11`

```text
5477: func appendBytesNoZero(b []byte, p pointer, f *coderFieldInfo, opts marshalOptions) ([]byte, error) {
5478: 	v := *p.Bytes()
5479: 	if len(v) == 0 {
5480: 		return b, nil
5481: 	}
5482: 	b = protowire.AppendVarint(b, f.wiretag)
5483: 	b = protowire.AppendBytes(b, v)
5484: 	return b, nil
```

## `deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_gen.go:5488-5499`

SHA256: `4eed98966d930db1855cadf0972d255cd89733e29815ef4d3904e3fe0e473a25`; binding: `google.golang.org_protobuf@v1.36.11`

```text
5488: // The zero value is not decoded.
5489: func consumeBytesNoZero(b []byte, p pointer, wtyp protowire.Type, f *coderFieldInfo, opts unmarshalOptions) (out unmarshalOutput, err error) {
5490: 	if wtyp != protowire.BytesType {
5491: 		return out, errUnknown
5492: 	}
5493: 	v, n := protowire.ConsumeBytes(b)
5494: 	if n < 0 {
5495: 		return out, errDecode
5496: 	}
5497: 	*p.Bytes() = append(([]byte)(nil), v...)
5498: 	out.n = n
5499: 	return out, nil
```

## `deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_tables.go:124-125`

SHA256: `8d4cdbd1367e0c089ecada4431bdb0b2f3198aa3907c25887131650adf78d29b`; binding: `google.golang.org_protobuf@v1.36.11`

```text
124: 		case protoreflect.MessageKind:
125: 			return getMessageInfo(ft), makeMessageSliceFieldCoder(fd, ft)
```

## `deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_tables.go:273-278`

SHA256: `8d4cdbd1367e0c089ecada4431bdb0b2f3198aa3907c25887131650adf78d29b`; binding: `google.golang.org_protobuf@v1.36.11`

```text
273: 		case protoreflect.BytesKind:
274: 			if ft.Kind() == reflect.String {
275: 				return nil, coderStringNoZero
276: 			}
277: 			if ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Uint8 {
278: 				return nil, coderBytesNoZero
```

## `deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_tables.go:423-428`

SHA256: `8d4cdbd1367e0c089ecada4431bdb0b2f3198aa3907c25887131650adf78d29b`; binding: `google.golang.org_protobuf@v1.36.11`

```text
423: 		case protoreflect.BytesKind:
424: 			if ft.Kind() == reflect.String {
425: 				return nil, coderString
426: 			}
427: 			if ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Uint8 {
428: 				return nil, coderBytes
```

## `deps/google.golang.org_protobuf@v1.36.11/internal/impl/merge.go:187-195`

SHA256: `5406a959d280825d0156c8e2e2857bd4e943a1aa0476a44717339ecb8d580628`; binding: `google.golang.org_protobuf@v1.36.11`

```text
187: func mergeBytes(dst, src pointer, _ *coderFieldInfo, _ mergeOptions) {
188: 	*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)
189: }
190: 
191: func mergeBytesNoZero(dst, src pointer, _ *coderFieldInfo, _ mergeOptions) {
192: 	v := *src.Bytes()
193: 	if len(v) > 0 {
194: 		*dst.Bytes() = append(emptyBuf[:], v...)
195: 	}
```

## `deps/google.golang.org_protobuf@v1.36.11/proto/decode.go:100-122`

SHA256: `5259b0da78b384fcce573d774277f13e83a1f06b7fd212dd5f6fc617b3941917`; binding: `google.golang.org_protobuf@v1.36.11`

```text
100: 	methods := protoMethods(m)
101: 	if methods != nil && methods.Unmarshal != nil &&
102: 		!(o.DiscardUnknown && methods.Flags&protoiface.SupportUnmarshalDiscardUnknown == 0) {
103: 		in := protoiface.UnmarshalInput{
104: 			Message:  m,
105: 			Buf:      b,
106: 			Resolver: o.Resolver,
107: 			Depth:    o.RecursionLimit,
108: 		}
109: 		if o.DiscardUnknown {
110: 			in.Flags |= protoiface.UnmarshalDiscardUnknown
111: 		}
112: 
113: 		if !allowPartial {
114: 			// This does not affect how current unmarshal functions work, it just allows them
115: 			// to record this for lazy the decoding case.
116: 			in.Flags |= protoiface.UnmarshalCheckRequired
117: 		}
118: 		if o.NoLazyDecoding {
119: 			in.Flags |= protoiface.UnmarshalNoLazyDecoding
120: 		}
121: 
122: 		out, err = methods.Unmarshal(in)
```

## `deps/google.golang.org_protobuf@v1.36.11/proto/encode.go:177-195`

SHA256: `e9a13c44a81991067d003f235234fa8a859ab1f78ff0430212861f5027d5a434`; binding: `google.golang.org_protobuf@v1.36.11`

```text
177: 	if methods := protoMethods(m); methods != nil && methods.Marshal != nil &&
178: 		!(o.Deterministic && methods.Flags&protoiface.SupportMarshalDeterministic == 0) {
179: 		in := protoiface.MarshalInput{
180: 			Message: m,
181: 			Buf:     b,
182: 			Flags:   o.flags(),
183: 		}
184: 		if methods.Size != nil {
185: 			sout := methods.Size(protoiface.SizeInput{
186: 				Message: m,
187: 				Flags:   in.Flags,
188: 			})
189: 			if cap(b) < len(b)+sout.Size {
190: 				in.Buf = make([]byte, len(b), growcap(cap(b), len(b)+sout.Size))
191: 				copy(in.Buf, b)
192: 			}
193: 			in.Flags |= protoiface.MarshalUseCachedSize
194: 		}
195: 		out, err = methods.Marshal(in)
```

## `deps/google.golang.org_protobuf@v1.36.11/proto/merge.go:41-59`

SHA256: `f5c50384cb64947ee71b6e77633c4c88f277575b36121bd7c8ee9f38fb6a16b7`; binding: `google.golang.org_protobuf@v1.36.11`

```text
41: func Clone(m Message) Message {
42: 	// NOTE: Most usages of Clone assume the following properties:
43: 	//	t := reflect.TypeOf(m)
44: 	//	t == reflect.TypeOf(m.ProtoReflect().New().Interface())
45: 	//	t == reflect.TypeOf(m.ProtoReflect().Type().Zero().Interface())
46: 	//
47: 	// Embedding protobuf messages breaks this since the parent type will have
48: 	// a forwarded ProtoReflect method, but the Interface method will return
49: 	// the underlying embedded message type.
50: 	if m == nil {
51: 		return nil
52: 	}
53: 	src := m.ProtoReflect()
54: 	if !src.IsValid() {
55: 		return src.Type().Zero().Interface()
56: 	}
57: 	dst := src.New()
58: 	mergeOptions{}.mergeMessage(dst, src)
59: 	return dst.Interface()
```

## `deps/google.golang.org_protobuf@v1.36.11/proto/merge.go:72-82`

SHA256: `f5c50384cb64947ee71b6e77633c4c88f277575b36121bd7c8ee9f38fb6a16b7`; binding: `google.golang.org_protobuf@v1.36.11`

```text
72: func (o mergeOptions) mergeMessage(dst, src protoreflect.Message) {
73: 	methods := protoMethods(dst)
74: 	if methods != nil && methods.Merge != nil {
75: 		in := protoiface.MergeInput{
76: 			Destination: dst,
77: 			Source:      src,
78: 		}
79: 		out := methods.Merge(in)
80: 		if out.Flags&protoiface.MergeComplete != 0 {
81: 			return
82: 		}
```

## `inputs/versions.txt:18`

SHA256: `f9a7aab30c2eacf44219fde0f7a1e956f271239dcf8fbad5a47b93030eca1494`; binding: `input-manifest.json`

```text
18: 	dep	go.etcd.io/etcd/api/v3	v3.7.2	h1:xgt/6el1LsPWWYNLkhMAK4tZm6dF+1sCqDecpE5gdbk=
```

## `inputs/versions.txt:43`

SHA256: `f9a7aab30c2eacf44219fde0f7a1e956f271239dcf8fbad5a47b93030eca1494`; binding: `input-manifest.json`

```text
43: 	dep	go.etcd.io/etcd/api/v3	v3.7.2	h1:xgt/6el1LsPWWYNLkhMAK4tZm6dF+1sCqDecpE5gdbk=
```

## `inputs/versions.txt:67`

SHA256: `f9a7aab30c2eacf44219fde0f7a1e956f271239dcf8fbad5a47b93030eca1494`; binding: `input-manifest.json`

```text
67: 	dep	go.etcd.io/etcd/api/v3	v3.7.2	h1:xgt/6el1LsPWWYNLkhMAK4tZm6dF+1sCqDecpE5gdbk=
```

## `inputs/workload/cas.go:14-19`

SHA256: `7d21344b5cf934ba8febdf1246628f69a326032a51dd985a65233a33e8656b83`; binding: `input-manifest.json`

```text
14: func casRequest(key, value []byte, revision int64) *pb.TxnRequest {
15: 	return &pb.TxnRequest{
16: 		Compare: []*pb.Compare{{Key: key, Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: revision}}},
17: 		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: value}}}},
18: 		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}}},
19: 	}
```

## `inputs/workload/cas.go:22-46`

SHA256: `7d21344b5cf934ba8febdf1246628f69a326032a51dd985a65233a33e8656b83`; binding: `input-manifest.json`

```text
22: func seedRevisions(client pb.KVClient, keys [][]byte) ([]atomic.Int64, error) {
23: 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
24: 	defer cancel()
25: 	r, err := client.Range(ctx, &pb.RangeRequest{Key: []byte("/registry/pods/"), RangeEnd: []byte("/registry/pods0"), KeysOnly: true})
26: 	if err != nil {
27: 		return nil, err
28: 	}
29: 	if r == nil || r.Header == nil || r.More || len(r.Kvs) != len(keys) || r.Count != int64(len(keys)) {
30: 		return nil, errors.New("CAS requires every seeded key to exist")
31: 	}
32: 	byKey := make(map[string]int64, len(keys))
33: 	for _, kv := range r.Kvs {
34: 		if kv.ModRevision <= 0 {
35: 			return nil, errors.New("invalid seeded mod revision")
36: 		}
37: 		byKey[string(kv.Key)] = kv.ModRevision
38: 	}
39: 	revisions := make([]atomic.Int64, len(keys))
40: 	for i, key := range keys {
41: 		revision, ok := byKey[string(key)]
42: 		if !ok {
43: 			return nil, fmt.Errorf("missing CAS seed key %s", key)
44: 		}
45: 		revisions[i].Store(revision)
46: 	}
```

## `inputs/workload/main.go:115-120`

SHA256: `1dc2ad26ce1ff3630d1167da3a1621c4ebe6415113c2cdb3dcd4e24181d90c99`; binding: `input-manifest.json`

```text
115: 	const keyspace = 4096
116: 	keys := make([][]byte, keyspace)
117: 	values := make([][]byte, keyspace)
118: 	for i := range keyspace {
119: 		keys[i] = []byte(fmt.Sprintf("/registry/pods/ns-%03d/pod-%08d", i%64, i))
120: 		values[i] = make([]byte, 1024)
```

## `inputs/workload/main.go:119`

SHA256: `1dc2ad26ce1ff3630d1167da3a1621c4ebe6415113c2cdb3dcd4e24181d90c99`; binding: `input-manifest.json`

```text
119: 		keys[i] = []byte(fmt.Sprintf("/registry/pods/ns-%03d/pod-%08d", i%64, i))
```

## `inputs/workload/main.go:178-205`

SHA256: `1dc2ad26ce1ff3630d1167da3a1621c4ebe6415113c2cdb3dcd4e24181d90c99`; binding: `input-manifest.json`

```text
178: 				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
179: 				var err error
180: 				var h *pb.ResponseHeader
181: 				if *op == "put" {
182: 					var r *pb.PutResponse
183: 					r, err = clients[w%*conns].Put(ctx, &pb.PutRequest{Key: keys[k], Value: values[k]})
184: 					if r != nil {
185: 						h = r.Header
186: 					}
187: 				} else if *op == "cas" {
188: 					var r *pb.TxnResponse
189: 					r, err = clients[w%*conns].Txn(ctx, casRequest(keys[k], values[k], revisions[k].Load()))
190: 					if r != nil {
191: 						h = r.Header
192: 						if err == nil {
193: 							var revision int64
194: 							revision, err = checkCAS(r, keys[k], values[k])
195: 							if err == nil {
196: 								storeRevision(&revisions[k], revision)
197: 								if r.Succeeded {
198: 									passes++
199: 								} else {
200: 									failures++
201: 								}
202: 							}
203: 						}
204: 					}
205: 				} else {
```

## `source/api/etcdserverpb/raft_internal.pb.go:14-17`

SHA256: `eb6933959deb7828f4b74077218c7924b9e44c3a33ea7a2cc87f9e2a6b8dcbf4`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
14: 	membershippb "go.etcd.io/etcd/api/v3/membershippb"
15: 	_ "go.etcd.io/etcd/api/v3/versionpb"
16: 	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
17: 	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
```

## `source/api/etcdserverpb/rpc.pb.go:1-26`

SHA256: `06269c48f068fbf6e7275760aa5865800afce354aef973e342a9c10131045401`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
1: // Code generated by protoc-gen-go. DO NOT EDIT.
2: // versions:
3: // 	protoc-gen-go v1.36.11
4: // 	protoc        v3.20.3
5: // source: rpc.proto
6: 
7: package etcdserverpb
8: 
9: import (
10: 	reflect "reflect"
11: 	sync "sync"
12: 	unsafe "unsafe"
13: 
14: 	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2/options"
15: 	authpb "go.etcd.io/etcd/api/v3/authpb"
16: 	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
17: 	_ "go.etcd.io/etcd/api/v3/versionpb"
18: 	_ "google.golang.org/genproto/googleapis/api/annotations"
19: 	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
20: 	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
21: )
22: 
23: const (
24: 	// Verify that this generated code is sufficiently up-to-date.
25: 	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
26: 	// Verify that runtime/protoimpl is sufficiently up-to-date.
```

## `source/api/etcdserverpb/rpc.pb.go:755-760`

SHA256: `06269c48f068fbf6e7275760aa5865800afce354aef973e342a9c10131045401`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
755: type PutRequest struct {
756: 	state protoimpl.MessageState `protogen:"open.v1"`
757: 	// key is the key, in bytes, to put into the key-value store.
758: 	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
759: 	// value is the value, in bytes, to associate with the key in the key-value store.
760: 	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
```

## `source/api/etcdserverpb/rpc.pb.go:1451-1462`

SHA256: `06269c48f068fbf6e7275760aa5865800afce354aef973e342a9c10131045401`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
1451: type TxnRequest struct {
1452: 	state protoimpl.MessageState `protogen:"open.v1"`
1453: 	// compare is a list of predicates representing a conjunction of terms.
1454: 	// If the comparisons succeed, then the success requests will be processed in order,
1455: 	// and the response will contain their respective responses in order.
1456: 	// If the comparisons fail, then the failure requests will be processed in order,
1457: 	// and the response will contain their respective responses in order.
1458: 	Compare []*Compare `protobuf:"bytes,1,rep,name=compare,proto3" json:"compare,omitempty"`
1459: 	// success is a list of requests which will be applied when compare evaluates to true.
1460: 	Success []*RequestOp `protobuf:"bytes,2,rep,name=success,proto3" json:"success,omitempty"`
1461: 	// failure is a list of requests which will be applied when compare evaluates to false.
1462: 	Failure       []*RequestOp `protobuf:"bytes,3,rep,name=failure,proto3" json:"failure,omitempty"`
```

## `source/api/etcdserverpb/rpc_grpc.pb.go:246-250`

SHA256: `daa7241561e7e626f7deb9bc92ade10c2bc09a7a83192a14fbabe9f6b88d1f60`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
246: func _KV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
247: 	in := new(PutRequest)
248: 	if err := dec(in); err != nil {
249: 		return nil, err
250: 	}
```

## `source/api/etcdserverpb/rpc_grpc.pb.go:282-286`

SHA256: `daa7241561e7e626f7deb9bc92ade10c2bc09a7a83192a14fbabe9f6b88d1f60`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
282: func _KV_Txn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
283: 	in := new(TxnRequest)
284: 	if err := dec(in); err != nil {
285: 		return nil, err
286: 	}
```

## `source/api/mvccpb/kv.pb.go:13-15`

SHA256: `8d847fd2e8ce2392c67d03663c3cbca202a5241be35d3046f6fd72df821833b3`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
13: 
14: 	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
15: 	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
```

## `source/api/mvccpb/kv.pb.go:79-84`

SHA256: `8d847fd2e8ce2392c67d03663c3cbca202a5241be35d3046f6fd72df821833b3`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
79: 	// version is the version of the key. A deletion resets
80: 	// the version to zero and any modification of the key
81: 	// increases its version.
82: 	Version int64 `protobuf:"varint,4,opt,name=version,proto3" json:"version,omitempty"`
83: 	// value is the value held by the key, in bytes.
84: 	Value []byte `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`
```

## `source/pkg/ioutil/pagewriter.go:59-62`

SHA256: `c7aa84a8499ad1f6223d37a845e333cc6994ac9e722cc8e533141d3a2924353b`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
59: 		copy(pw.buf[pw.bufferedBytes:], p)
60: 		pw.bufferedBytes += len(p)
61: 		return len(p), nil
62: 	}
```

## `source/pkg/ioutil/pagewriter.go:66-76`

SHA256: `c7aa84a8499ad1f6223d37a845e333cc6994ac9e722cc8e533141d3a2924353b`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
66: 		partial := slack > len(p)
67: 		if partial {
68: 			// not enough data to complete the slack page
69: 			slack = len(p)
70: 		}
71: 		// special case: writing to slack page in buffer
72: 		copy(pw.buf[pw.bufferedBytes:], p[:slack])
73: 		pw.bufferedBytes += slack
74: 		n = slack
75: 		p = p[slack:]
76: 		if partial {
```

## `source/pkg/ioutil/pagewriter.go:89-100`

SHA256: `c7aa84a8499ad1f6223d37a845e333cc6994ac9e722cc8e533141d3a2924353b`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
89: 		n += c
90: 		if werr != nil {
91: 			return n, werr
92: 		}
93: 		p = p[pages*pw.pageBytes:]
94: 	}
95: 	// write remaining tail to buffer
96: 	c, werr := pw.Write(p)
97: 	n += c
98: 	return n, werr
99: }
100: 
```

## `source/pkg/pbutil/pbutil.go:53-58`

SHA256: `f61795ced1b395f337eada0415ccbad7888c0826bd3637b14ff22b8e9026d198`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
53: func MustMarshalMessage(m proto.Message) []byte {
54: 	d, err := proto.Marshal(m)
55: 	if err != nil {
56: 		panic(fmt.Sprintf("marshal should never fail (%v)", err))
57: 	}
58: 	return d
```

## `source/pkg/pbutil/pbutil.go:61-65`

SHA256: `f61795ced1b395f337eada0415ccbad7888c0826bd3637b14ff22b8e9026d198`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
61: func MustUnmarshalMessage(um proto.Message, data []byte) {
62: 	if err := proto.Unmarshal(data, um); err != nil {
63: 		panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
64: 	}
65: }
```

## `source/pkg/schedule/schedule.go:114-120`

SHA256: `0a017f7cf39bd265e72c7f2d519d6768c5973e2c716abea15e7fab0e65e54ea2`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
114: 	if len(f.pendings) == 0 {
115: 		select {
116: 		case f.resume <- struct{}{}:
117: 		default:
118: 		}
119: 	}
120: 	f.pendings = append(f.pendings, j)
```

## `source/pkg/schedule/schedule.go:174`

SHA256: `0a017f7cf39bd265e72c7f2d519d6768c5973e2c716abea15e7fab0e65e54ea2`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
174: 			case <-f.resume:
```

## `source/pkg/wait/wait.go:65`

SHA256: `93124fec939c6484a54971a063bacbf9a7b9a715296ddd747acaac44d40d4af0`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
65: 	newCh := make(chan any, 1)
```

## `source/pkg/wait/wait.go:83-84`

SHA256: `93124fec939c6484a54971a063bacbf9a7b9a715296ddd747acaac44d40d4af0`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
83: 		ch <- x
84: 		close(ch)
```

## `source/pkg/wait/wait_time.go:56-64`

SHA256: `11e562c666a6f8dc3b8dda805e29c02d6704d29d549914385aadc7c121e4002b`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
56: func (tl *timeList) Trigger(deadline uint64) {
57: 	tl.l.Lock()
58: 	defer tl.l.Unlock()
59: 	tl.lastTriggerDeadline = deadline
60: 	for t, ch := range tl.m {
61: 		if t <= deadline {
62: 			delete(tl.m, t)
63: 			close(ch)
64: 		}
```

## `source/server/etcdserver/api/rafthttp/msg_codec.go:38`

SHA256: `6cd135371bdaec67b24edc76eacec3659ca1bcfd0e91a75e28f7b663aee43556`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
38: 	_, err := enc.w.Write(pbutil.MustMarshalMessage(m))
```

## `source/server/etcdserver/api/rafthttp/msg_codec.go:69`

SHA256: `6cd135371bdaec67b24edc76eacec3659ca1bcfd0e91a75e28f7b663aee43556`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
69: 	return &m, proto.Unmarshal(buf, &m)
```

## `source/server/etcdserver/api/rafthttp/msgappv2_codec.go:114`

SHA256: `cb4a18507ef875e4b809674223d37d583f5187a209947d2a70f2c390ba8a23e1`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
114: 				b, err := opts.MarshalAppend(enc.buf[:0], m.Entries[i])
```

## `source/server/etcdserver/api/rafthttp/msgappv2_codec.go:122`

SHA256: `cb4a18507ef875e4b809674223d37d583f5187a209947d2a70f2c390ba8a23e1`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
122: 				if _, err := enc.w.Write(pbutil.MustMarshalMessage(m.Entries[i])); err != nil {
```

## `source/server/etcdserver/api/rafthttp/msgappv2_codec.go:143`

SHA256: `cb4a18507ef875e4b809674223d37d583f5187a209947d2a70f2c390ba8a23e1`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
143: 		if _, err := enc.w.Write(pbutil.MustMarshalMessage(m)); err != nil {
```

## `source/server/etcdserver/api/rafthttp/msgappv2_codec.go:227`

SHA256: `cb4a18507ef875e4b809674223d37d583f5187a209947d2a70f2c390ba8a23e1`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
227: 			pbutil.MustUnmarshalMessage(m.Entries[i], buf)
```

## `source/server/etcdserver/api/rafthttp/msgappv2_codec.go:243`

SHA256: `cb4a18507ef875e4b809674223d37d583f5187a209947d2a70f2c390ba8a23e1`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
243: 		pbutil.MustUnmarshalMessage(&m, buf)
```

## `source/server/etcdserver/api/rafthttp/peer.go:177-178`

SHA256: `e8fd9f9ffb0648c308685b8411b940f0d7cdf202c28e4782c0e8174430b40cb7`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
177: 			case mm := <-p.recvc:
178: 				if err := r.Process(ctx, mm); err != nil {
```

## `source/server/etcdserver/api/rafthttp/peer.go:247`

SHA256: `e8fd9f9ffb0648c308685b8411b940f0d7cdf202c28e4782c0e8174430b40cb7`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
247: 	case writec <- m:
```

## `source/server/etcdserver/api/rafthttp/stream.go:207`

SHA256: `6c55eeeea44625c477524cee9d4df80a641c28b1843ead8e2f2183f8da045ae6`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
207: 		case m := <-msgc:
```

## `source/server/etcdserver/api/rafthttp/stream.go:530`

SHA256: `6c55eeeea44625c477524cee9d4df80a641c28b1843ead8e2f2183f8da045ae6`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
530: 		case recvc <- m:
```

## `source/server/etcdserver/apply/apply.go:27-29`

SHA256: `3432a495439627341f70a2d9f76675665d9add63fa599b034869293a1c5be986`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
27: func Apply(lg *zap.Logger, e *raftpb.Entry, uberApply UberApplier, w wait.Wait, shouldApplyV3 membership.ShouldApplyV3) (ar *Result, id uint64) {
28: 	var raftReq pb.InternalRaftRequest
29: 	pbutil.MustUnmarshalMessage(&raftReq, e.Data)
```

## `source/server/etcdserver/apply/apply.go:40-46`

SHA256: `3432a495439627341f70a2d9f76675665d9add63fa599b034869293a1c5be986`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
40: 	needResult := w.IsRegistered(id)
41: 	wrapper := &InternalRaftRequestWrapper{
42: 		InternalRaftRequest: &raftReq,
43: 		SkipRangeExecution:  !needResult && raftReq.Txn != nil,
44: 	}
45: 	if needResult || !noSideEffect(&raftReq) {
46: 		return uberApply.Apply(wrapper, shouldApplyV3), id
```

## `source/server/etcdserver/bootstrap.go:540-552`

SHA256: `8e260cd55ce8de63a6c398ed104a30405fe6ad306a76686a26a47f7896083115`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
540: func raftConfig(cfg config.ServerConfig, id uint64, s *raft.MemoryStorage) *raft.Config {
541: 	return &raft.Config{
542: 		ID:              id,
543: 		ElectionTick:    cfg.ElectionTicks,
544: 		HeartbeatTick:   1,
545: 		Storage:         s,
546: 		MaxSizePerMsg:   maxSizePerMsg,
547: 		MaxInflightMsgs: maxInflightMsgs,
548: 		CheckQuorum:     true,
549: 		PreVote:         cfg.PreVote,
550: 		Logger:          NewRaftLoggerZap(cfg.Logger.Named("raft")),
551: 	}
552: }
```

## `source/server/etcdserver/raft.go:183`

SHA256: `37853a3bdf903810058a335df96e96874673a5974d9e5e5adfea96d9a52bca42`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
183: 			case <-r.ticker.C:
```

## `source/server/etcdserver/raft.go:185`

SHA256: `37853a3bdf903810058a335df96e96874673a5974d9e5e5adfea96d9a52bca42`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
185: 			case rd := <-r.Ready():
```

## `source/server/etcdserver/raft.go:219-220`

SHA256: `37853a3bdf903810058a335df96e96874673a5974d9e5e5adfea96d9a52bca42`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
219: 				notifyc := make(chan struct{}, 1)
220: 				raftAdvancedC := make(chan struct{}, 1)
```

## `source/server/etcdserver/raft.go:232`

SHA256: `37853a3bdf903810058a335df96e96874673a5974d9e5e5adfea96d9a52bca42`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
232: 				case r.applyc <- ap:
```

## `source/server/etcdserver/raft.go:255-257`

SHA256: `37853a3bdf903810058a335df96e96874673a5974d9e5e5adfea96d9a52bca42`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
255: 				// gofail: var raftBeforeSave struct{}
256: 				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
257: 					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
```

## `source/server/etcdserver/raft.go:302`

SHA256: `37853a3bdf903810058a335df96e96874673a5974d9e5e5adfea96d9a52bca42`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
302: 					notifyc <- struct{}{}
```

## `source/server/etcdserver/raft.go:327`

SHA256: `37853a3bdf903810058a335df96e96874673a5974d9e5e5adfea96d9a52bca42`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
327: 					notifyc <- struct{}{}
```

## `source/server/etcdserver/server.go:843`

SHA256: `2313798e367fc6ae74c0a10643124ed9b6b7b1a95eedfc7929addda6f881636d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
843: 		case ap := <-s.r.apply():
```

## `source/server/etcdserver/server.go:844-845`

SHA256: `2313798e367fc6ae74c0a10643124ed9b6b7b1a95eedfc7929addda6f881636d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
844: 			f := schedule.NewJob("server_applyAll", func(context.Context) { s.applyAll(&ep, &ap) })
845: 			sched.Schedule(f)
```

## `source/server/etcdserver/server.go:983`

SHA256: `2313798e367fc6ae74c0a10643124ed9b6b7b1a95eedfc7929addda6f881636d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
983: 	<-apply.notifyc
```

## `source/server/etcdserver/server.go:1970-1983`

SHA256: `2313798e367fc6ae74c0a10643124ed9b6b7b1a95eedfc7929addda6f881636d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
1970: 	ar, id := apply.Apply(s.lg, e, s.uberApply, s.w, shouldApplyV3)
1971: 
1972: 	// do not re-toApply applied entries.
1973: 	if !shouldApplyV3 {
1974: 		return
1975: 	}
1976: 
1977: 	if ar == nil {
1978: 		return
1979: 	}
1980: 
1981: 	if !errorspkg.Is(ar.Err, errors.ErrNoSpace) || len(s.alarmStore.Get(pb.AlarmType_NOSPACE)) > 0 {
1982: 		s.w.Trigger(id, ar)
1983: 		return
```

## `source/server/etcdserver/txn/put.go:52-66`

SHA256: `b11b93fb995c8f5334284061c86bd4f332a261471dc80da8174cdf0d27aed3cc`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
52: 	val, leaseID := p.Value, lease.LeaseID(p.Lease)
53: 
54: 	if p.IgnoreValue {
55: 		val = prevKV.KVs[0].Value
56: 	}
57: 	if p.IgnoreLease {
58: 		leaseID = lease.LeaseID(prevKV.KVs[0].Lease)
59: 	}
60: 	if p.PrevKv {
61: 		if prevKV != nil && len(prevKV.KVs) != 0 {
62: 			resp.PrevKv = prevKV.KVs[0]
63: 		}
64: 	}
65: 
66: 	resp.Header.Revision = txnWrite.Put(p.Key, val, leaseID)
```

## `source/server/etcdserver/txn/put.go:104-115`

SHA256: `b11b93fb995c8f5334284061c86bd4f332a261471dc80da8174cdf0d27aed3cc`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
104: func getPrevKV(trace *traceutil.Trace, txnWrite mvcc.ReadView, p *pb.PutRequest) (prevKV *mvcc.RangeResult, err error) {
105: 	if p.IgnoreValue || p.IgnoreLease || p.PrevKv {
106: 		trace.StepWithFunction(func() {
107: 			prevKV, err = txnWrite.Range(context.TODO(), p.Key, nil, mvcc.RangeOptions{})
108: 		}, "get previous kv pair")
109: 
110: 		if err != nil {
111: 			return nil, err
112: 		}
113: 	}
114: 	return prevKV, nil
115: }
```

## `source/server/etcdserver/txn/range.go:62-71`

SHA256: `b77f1c62e0de98b43c30d7b3b4c32409c61900f53d07719632779d6249dea74a`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
62: 	limit := rangeLimit(r)
63: 	ro := mvcc.RangeOptions{
64: 		Limit:          limit,
65: 		Rev:            r.Revision,
66: 		CountOnly:      r.CountOnly,
67: 		FastKeysOnly:   r.KeysOnly && r.SortTarget != pb.RangeRequest_VALUE,
68: 		WithTotalCount: withTotalCount,
69: 	}
70: 
71: 	rr, err := txnRead.Range(ctx, r.Key, mkGteRange(r.RangeEnd), ro)
```

## `source/server/etcdserver/txn/txn.go:137-162`

SHA256: `6e895f9d3f5794a308881cce4b50878054e6f93e794506ca570042cefa7501bf`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
137: 	reqs := rt.Success
138: 	if !txnPath[0] {
139: 		reqs = rt.Failure
140: 	}
141: 
142: 	for i, req := range reqs {
143: 		respi := tresp.Responses[i].Response
144: 		switch tv := req.Request.(type) {
145: 		case *pb.RequestOp_RequestRange:
146: 			trace.StartSubTrace(
147: 				traceutil.Field{Key: "req_type", Value: "range"},
148: 				traceutil.Field{Key: "range_begin", Value: string(tv.RequestRange.Key)},
149: 				traceutil.Field{Key: "range_end", Value: string(tv.RequestRange.RangeEnd)})
150: 			var resp *pb.RangeResponse
151: 			if skipRangeExecution {
152: 				rev := txnWrite.Rev()
153: 				if len(txnWrite.Changes()) > 0 {
154: 					rev++
155: 				}
156: 				resp = &pb.RangeResponse{
157: 					Header: &pb.ResponseHeader{Revision: rev},
158: 				}
159: 			} else {
160: 				resp, err = executeRange(ctx, lg, txnWrite, tv.RequestRange, true)
161: 				if err != nil {
162: 					return 0, fmt.Errorf("applyTxn: failed Range: %w", err)
```

## `source/server/etcdserver/txn/txn.go:151-162`

SHA256: `6e895f9d3f5794a308881cce4b50878054e6f93e794506ca570042cefa7501bf`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
151: 			if skipRangeExecution {
152: 				rev := txnWrite.Rev()
153: 				if len(txnWrite.Changes()) > 0 {
154: 					rev++
155: 				}
156: 				resp = &pb.RangeResponse{
157: 					Header: &pb.ResponseHeader{Revision: rev},
158: 				}
159: 			} else {
160: 				resp, err = executeRange(ctx, lg, txnWrite, tv.RequestRange, true)
161: 				if err != nil {
162: 					return 0, fmt.Errorf("applyTxn: failed Range: %w", err)
```

## `source/server/etcdserver/txn/txn.go:268-291`

SHA256: `6e895f9d3f5794a308881cce4b50878054e6f93e794506ca570042cefa7501bf`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
268: func applyCompare(rv mvcc.ReadView, c *pb.Compare) bool {
269: 	// TODO: possible optimizations
270: 	// * chunk reads for large ranges to conserve memory
271: 	// * rewrite rules for common patterns:
272: 	//	ex. "[a, b) createrev > 0" => "limit 1 /\ kvs > 0"
273: 	// * caching
274: 	rr, err := rv.Range(context.TODO(), c.Key, mkGteRange(c.RangeEnd), mvcc.RangeOptions{})
275: 	if err != nil {
276: 		return false
277: 	}
278: 	if len(rr.KVs) == 0 {
279: 		if c.Target == pb.Compare_VALUE {
280: 			// Always fail if comparing a value on a key/keys that doesn't exist;
281: 			// nil == empty string in grpc; no way to represent missing value
282: 			return false
283: 		}
284: 		return compareKV(c, &mvccpb.KeyValue{})
285: 	}
286: 	for _, kv := range rr.KVs {
287: 		if !compareKV(c, kv) {
288: 			return false
289: 		}
290: 	}
291: 	return true
```

## `source/server/etcdserver/txn/txn.go:312-316`

SHA256: `6e895f9d3f5794a308881cce4b50878054e6f93e794506ca570042cefa7501bf`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
312: 		}
313: 		result = compareInt64(ckv.ModRevision, rev)
314: 	case pb.Compare_VERSION:
315: 		if tv, _ := c.TargetUnion.(*pb.Compare_Version); tv != nil {
316: 			rev = tv.Version
```

## `source/server/etcdserver/v3_server.go:303`

SHA256: `fa0e44987280ac775c53f070a44a43af7b70291a4579c329cf9db53e4b9ae495`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
303: 	resp, err := s.raftRequest(ctx, &pb.InternalRaftRequest{Put: r})
```

## `source/server/etcdserver/v3_server.go:325-326`

SHA256: `fa0e44987280ac775c53f070a44a43af7b70291a4579c329cf9db53e4b9ae495`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
325: func (s *EtcdServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
326: 	readOnly := txn.IsTxnReadonly(r)
```

## `source/server/etcdserver/v3_server.go:375`

SHA256: `fa0e44987280ac775c53f070a44a43af7b70291a4579c329cf9db53e4b9ae495`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
375: 	resp, err := s.raftRequest(ctx, &pb.InternalRaftRequest{Txn: r})
```

## `source/server/etcdserver/v3_server.go:1093`

SHA256: `fa0e44987280ac775c53f070a44a43af7b70291a4579c329cf9db53e4b9ae495`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
1093: 	data, err = proto.Marshal(r)
```

## `source/server/etcdserver/v3_server.go:1106-1113`

SHA256: `fa0e44987280ac775c53f070a44a43af7b70291a4579c329cf9db53e4b9ae495`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
1106: 	ch := s.w.Register(id)
1107: 
1108: 	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
1109: 	defer cancel()
1110: 
1111: 	span := trace.SpanFromContext(ctx)
1112: 	span.AddEvent("Send raft proposal")
1113: 	err = s.r.Propose(cctx, data)
```

## `source/server/etcdserver/v3_server.go:1108-1109`

SHA256: `fa0e44987280ac775c53f070a44a43af7b70291a4579c329cf9db53e4b9ae495`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
1108: 	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
1109: 	defer cancel()
```

## `source/server/etcdserver/v3_server.go:1123`

SHA256: `fa0e44987280ac775c53f070a44a43af7b70291a4579c329cf9db53e4b9ae495`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
1123: 	case x := <-ch:
```

## `source/server/go.mod:25-42`

SHA256: `a3690b8b60b7ef221b50bda3a81ed35f1c81af02cfbacf10ddeffe1dc037f47c`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
25: 	go.etcd.io/bbolt v1.5.0
26: 	go.etcd.io/etcd/api/v3 v3.7.2
27: 	go.etcd.io/etcd/client/pkg/v3 v3.7.2
28: 	go.etcd.io/etcd/client/v3 v3.7.2
29: 	go.etcd.io/etcd/pkg/v3 v3.7.2
30: 	go.etcd.io/raft/v3 v3.7.0
31: 	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.68.0
32: 	go.opentelemetry.io/otel v1.44.0
33: 	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.43.0
34: 	go.opentelemetry.io/otel/sdk v1.44.0
35: 	go.opentelemetry.io/otel/trace v1.44.0
36: 	go.uber.org/zap v1.27.1
37: 	golang.org/x/crypto v0.55.0
38: 	golang.org/x/net v0.58.0
39: 	golang.org/x/time v0.15.0
40: 	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa
41: 	google.golang.org/grpc v1.83.2
42: 	google.golang.org/protobuf v1.36.11
```

## `source/server/go.mod:26`

SHA256: `a3690b8b60b7ef221b50bda3a81ed35f1c81af02cfbacf10ddeffe1dc037f47c`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
26: 	go.etcd.io/etcd/api/v3 v3.7.2
```

## `source/server/go.mod:77-82`

SHA256: `a3690b8b60b7ef221b50bda3a81ed35f1c81af02cfbacf10ddeffe1dc037f47c`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
77: replace (
78: 	go.etcd.io/etcd/api/v3 => ../api
79: 	go.etcd.io/etcd/client/pkg/v3 => ../client/pkg
80: 	go.etcd.io/etcd/client/v3 => ../client/v3
81: 	go.etcd.io/etcd/pkg/v3 => ../pkg
82: )
```

## `source/server/storage/backend/backend.go:251`

SHA256: `1f3f9daed3b54e926d49e7b2987d5109d716b2953ddeedfbc1d35c2252c99142`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
251: 	b.batchTx = newBatchTxBuffered(b)
```

## `source/server/storage/backend/batch_tx.go:163`

SHA256: `1029fef579abe7e38c79e983f4c7e17d6e9d7fb3ef03cf78525d9beda6ed7f92`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
163: 	if err := bucket.Put(key, value); err != nil {
```

## `source/server/storage/backend/batch_tx.go:187-205`

SHA256: `1029fef579abe7e38c79e983f4c7e17d6e9d7fb3ef03cf78525d9beda6ed7f92`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
187: 	if limit <= 0 {
188: 		limit = math.MaxInt64
189: 	}
190: 	var isMatch func(b []byte) bool
191: 	if len(endKey) > 0 {
192: 		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
193: 	} else {
194: 		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
195: 		limit = 1
196: 	}
197: 
198: 	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
199: 		vs = append(vs, cv)
200: 		keys = append(keys, ck)
201: 		if limit == int64(len(keys)) {
202: 			break
203: 		}
204: 	}
205: 	return keys, vs
```

## `source/server/storage/backend/batch_tx.go:393-395`

SHA256: `1029fef579abe7e38c79e983f4c7e17d6e9d7fb3ef03cf78525d9beda6ed7f92`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
393: func (t *batchTxBuffered) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
394: 	t.batchTx.UnsafeSeqPut(bucket, key, value)
395: 	t.buf.putSeq(bucket, key, value)
```

## `source/server/storage/backend/read_tx.go:79-93`

SHA256: `eb2ea5cc7e3cf604615841f210ffafabbcb29249c5b1686048b71751b0a8c61d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
79: 	if endKey == nil {
80: 		// forbid duplicates for single keys
81: 		limit = 1
82: 	}
83: 	if limit <= 0 {
84: 		limit = math.MaxInt64
85: 	}
86: 	if limit > 1 && !bucketType.IsSafeRangeBucket() {
87: 		panic("do not use unsafeRange on non-keys bucket")
88: 	}
89: 	keys, vals := baseReadTx.buf.Range(bucketType, key, endKey, limit)
90: 	if int64(len(keys)) == limit {
91: 		return keys, vals
92: 	}
93: 
```

## `source/server/storage/backend/read_tx.go:120-121`

SHA256: `eb2ea5cc7e3cf604615841f210ffafabbcb29249c5b1686048b71751b0a8c61d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
120: 	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
121: 	return append(k2, keys...), append(v2, vals...)
```

## `source/server/storage/backend/tx_buffer.go:204-210`

SHA256: `36298b2792d79d4cc7a0b560d300a1f953c9c68b1137900305c55d626cc6fac0`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
204: func (bb *bucketBuffer) add(k, v []byte) {
205: 	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v
206: 	bb.used++
207: 	if bb.used == len(bb.buf) {
208: 		buf := make([]kv, (3*len(bb.buf))/2)
209: 		copy(buf, bb.buf)
210: 		bb.buf = buf
```

## `source/server/storage/mvcc/index.go:54-66`

SHA256: `c873dae7f6e1b8f442bfcd3ada8adaa522485f463096993360a7e1b38208a8f9`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
54: func (ti *treeIndex) Put(key []byte, rev Revision) {
55: 	keyi := &keyIndex{key: key}
56: 
57: 	ti.Lock()
58: 	defer ti.Unlock()
59: 	okeyi, ok := ti.tree.Get(keyi)
60: 	if !ok {
61: 		keyi.put(ti.lg, rev.Main, rev.Sub)
62: 		ti.tree.ReplaceOrInsert(keyi)
63: 		return
64: 	}
65: 	okeyi.put(ti.lg, rev.Main, rev.Sub)
66: }
```

## `source/server/storage/mvcc/kvstore_txn.go:130`

SHA256: `8127b4ce3049d078257294344d2e929a08034dae42fe35385f5ab0358a372d7d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
130: 				zap.Int64("revision-main", revpair.Main),
```

## `source/server/storage/mvcc/kvstore_txn.go:141-142`

SHA256: `8127b4ce3049d078257294344d2e929a08034dae42fe35385f5ab0358a372d7d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
141: 		kv := &mvccpb.KeyValue{}
142: 		if err := proto.Unmarshal(vs[0], kv); err != nil {
```

## `source/server/storage/mvcc/kvstore_txn.go:241-245`

SHA256: `8127b4ce3049d078257294344d2e929a08034dae42fe35385f5ab0358a372d7d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
241: 	kv := &mvccpb.KeyValue{
242: 		Key:            key,
243: 		Value:          value,
244: 		CreateRevision: c,
245: 		ModRevision:    rev,
```

## `source/server/storage/mvcc/kvstore_txn.go:241-259`

SHA256: `8127b4ce3049d078257294344d2e929a08034dae42fe35385f5ab0358a372d7d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
241: 	kv := &mvccpb.KeyValue{
242: 		Key:            key,
243: 		Value:          value,
244: 		CreateRevision: c,
245: 		ModRevision:    rev,
246: 		Version:        ver,
247: 		Lease:          int64(leaseID),
248: 	}
249: 
250: 	d, err := proto.Marshal(kv)
251: 	if err != nil {
252: 		tw.storeTxnCommon.s.lg.Fatal(
253: 			"failed to marshal mvccpb.KeyValue",
254: 			zap.Error(err),
255: 		)
256: 	}
257: 
258: 	tw.trace.Step("marshal mvccpb.KeyValue")
259: 	tw.tx.UnsafeSeqPut(schema.Key, ibytes, d)
```

## `source/server/storage/mvcc/kvstore_txn.go:259`

SHA256: `8127b4ce3049d078257294344d2e929a08034dae42fe35385f5ab0358a372d7d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
259: 	tw.tx.UnsafeSeqPut(schema.Key, ibytes, d)
```

## `source/server/storage/storage.go:91-94`

SHA256: `30ef7ed7407f481b1f33496246194d396f1a8bdb3573ca95c08518c6ef018d4d`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
91: func (st *storage) Save(s *raftpb.HardState, ents []*raftpb.Entry) error {
92: 	st.mux.RLock()
93: 	defer st.mux.RUnlock()
94: 	return st.w.Save(s, ents)
```

## `source/server/storage/wal/decoder.go:34`

SHA256: `93e0ea9427a3cab39975b431c1337c5ed7114b3836c7e18a848b2e744abeb093`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
34: const minSectorSize = 512
```

## `source/server/storage/wal/encoder.go:36`

SHA256: `2d1969c3eb3fdc8fded4a0e5ba69161ffed78c7bf552cbc345365d39c8e6d7e2`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
36: const walPageBytes = 8 * minSectorSize
```

## `source/server/storage/wal/encoder.go:49`

SHA256: `2d1969c3eb3fdc8fded4a0e5ba69161ffed78c7bf552cbc345365d39c8e6d7e2`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
49: 		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
```

## `source/server/storage/wal/encoder.go:81-93`

SHA256: `2d1969c3eb3fdc8fded4a0e5ba69161ffed78c7bf552cbc345365d39c8e6d7e2`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
81: 	size := proto.Size(rec)
82: 	opts := proto.MarshalOptions{UseCachedSize: true}
83: 	if size > len(e.buf) {
84: 		data, err = proto.Marshal(rec)
85: 		if err != nil {
86: 			return err
87: 		}
88: 	} else {
89: 		data, err = opts.MarshalAppend(e.buf[:0], rec)
90: 		if err != nil {
91: 			return err
92: 		}
93: 	}
```

## `source/server/storage/wal/encoder.go:94-96`

SHA256: `2d1969c3eb3fdc8fded4a0e5ba69161ffed78c7bf552cbc345365d39c8e6d7e2`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
94: 
95: 	data, lenField := prepareDataWithPadding(data)
96: 
```

## `source/server/storage/wal/encoder.go:135-136`

SHA256: `2d1969c3eb3fdc8fded4a0e5ba69161ffed78c7bf552cbc345365d39c8e6d7e2`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
135: 	// write the record with padding
136: 	n, err := w.Write(data)
```

## `source/server/storage/wal/wal.go:974-978`

SHA256: `8a718abf8d04c858223f1ac62c3913cb6625b467595d3a487a86ecd8915e2efa`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
974: func (w *WAL) saveEntry(e *raftpb.Entry) error {
975: 	// TODO: add MustMarshalTo to reduce one allocation.
976: 	b := pbutil.MustMarshalMessage(e)
977: 	rec := &walpb.Record{Type: new(EntryType), Data: b}
978: 	if err := w.encoder.encode(rec); err != nil {
```

## `source/server/storage/wal/wal.go:995-1028`

SHA256: `8a718abf8d04c858223f1ac62c3913cb6625b467595d3a487a86ecd8915e2efa`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
995: func (w *WAL) Save(st *raftpb.HardState, ents []*raftpb.Entry) error {
996: 	w.mu.Lock()
997: 	defer w.mu.Unlock()
998: 
999: 	// short cut, do not call sync
1000: 	if raft.IsEmptyHardState(st) && len(ents) == 0 {
1001: 		return nil
1002: 	}
1003: 	if st == nil {
1004: 		st = &raftpb.HardState{}
1005: 	}
1006: 	if w.state == nil {
1007: 		w.state = &raftpb.HardState{}
1008: 	}
1009: 
1010: 	mustSync := raft.MustSync(st, w.state, len(ents))
1011: 
1012: 	// TODO(xiangli): no more reference operator
1013: 	for i := range ents {
1014: 		if err := w.saveEntry(ents[i]); err != nil {
1015: 			return err
1016: 		}
1017: 	}
1018: 	if err := w.saveState(st); err != nil {
1019: 		return err
1020: 	}
1021: 
1022: 	curOff, err := w.tail().Seek(0, io.SeekCurrent)
1023: 	if err != nil {
1024: 		return err
1025: 	}
1026: 	if curOff < SegmentSizeBytes {
1027: 		if mustSync {
1028: 			// gofail: var walBeforeSync struct{}
```

## `source/server/storage/wal/walpb/record.pb.go:27-32`

SHA256: `4fed8b40f973a1d308171ef5cb9297db5c7d16a20e477ab7b3e2be2c1221c796`; binding: `68c065e562994b89e333e77b039ad066f933c586`

```text
27: 	state         protoimpl.MessageState `protogen:"open.v1"`
28: 	Type          *int64                 `protobuf:"varint,1,opt,name=type" json:"type,omitempty"`
29: 	Crc           *uint32                `protobuf:"varint,2,opt,name=crc" json:"crc,omitempty"`
30: 	Data          []byte                 `protobuf:"bytes,3,opt,name=data" json:"data,omitempty"`
31: 	unknownFields protoimpl.UnknownFields
32: 	sizeCache     protoimpl.SizeCache
```
