package sim

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/testcert"
)

// An image is saved to a file once and every fork of it starts from that
// file in a gosim run of its own (#794): a fork then starts in a fresh
// simulation, which no earlier fork or aging run left anything in, alone
// or in a campaign's sequence. The file holds all a fork takes of its
// image: when it was taken, its certificates, its disks as a power loss
// leaves them (simfs.Disk.Save), its members' IDs, the record of what its
// nodes committed, and its store (history.Start), or a synthetic image's.
// Every field is in the file's own binary form, so no byte of a key or a
// value changes on the way.

// imageMagic starts an image's file.
const imageMagic = "tigerstore sim image 2\n"

// errImageFile means that an image's file does not parse.
var errImageFile = errors.New("sim: malformed image file")

// save writes img to w.
func (img *image) save(w io.Writer) error {
	net := &img.src.net
	if net.ca != net.signer.Pool {
		// A rotated certificate's pool trusts more than its signer.
		return errors.New("sim: an image whose certificate was rotated cannot be saved")
	}
	ca, err := net.signer.Save()
	if err != nil {
		return err
	}
	cert, err := testcert.SaveCertificate(net.cert)
	if err != nil {
		return err
	}
	var synth []byte
	if img.synth != nil {
		if synth, err = json.Marshal(savedSynth{Rev: img.synth.rev, Seeds: img.synth.seeds, Leases: img.synth.leases}); err != nil {
			return err
		}
	}
	e := &imageEncoder{w: bufio.NewWriter(w)}
	e.bytes([]byte(imageMagic))
	e.int(img.at.UnixNano())
	e.bytes(ca)
	e.bytes(cert)
	e.uint(uint64(len(net.hosts)))
	for _, h := range net.hosts {
		e.bytes([]byte(h))
	}
	e.int(net.expires.UnixNano())
	e.int(int64(net.valid))
	e.uint(uint64(len(img.disks)))
	for i, d := range img.disks {
		// A disk's length comes before it: a first pass counts it, so that
		// no disk is held whole in memory on the way to the file.
		var n countWriter
		if err := d.Save(&n); err != nil {
			return err
		}
		e.uint(img.ids[i])
		e.uint(uint64(n))
		if e.err == nil {
			e.err = d.Save(e.w)
		}
	}
	e.uint(img.maxLogged)
	e.uint(img.applied)
	e.uint(uint64(img.ops))
	l := img.log
	e.uint(l.from)
	n := 0
	for j := range l.recs {
		if l.set[j] {
			n++
		}
	}
	e.uint(uint64(n))
	for j, r := range l.recs {
		if !l.set[j] {
			continue
		}
		e.uint(l.from + uint64(j))
		e.uint(r.term)
		e.uint(uint64(r.typ))
		e.bool(r.known)
		e.uint(r.hash)
		e.uint(r.node)
		e.bytes(l.bytes(r))
	}
	e.uint(uint64(len(img.ckpts)))
	for _, i := range slices.Sorted(maps.Keys(img.ckpts)) {
		e.uint(i)
		e.uint(img.ckpts[i].sum)
		e.uint(img.ckpts[i].node)
	}
	for _, m := range []map[int64]int64{img.expiredAt, img.revokedAt} {
		e.uint(uint64(len(m)))
		for _, id := range slices.Sorted(maps.Keys(m)) {
			e.int(id)
			e.int(m[id])
		}
	}
	st := img.start
	e.int(st.Rev)
	e.uint(uint64(len(st.KVs)))
	for _, kv := range st.KVs {
		e.bytes([]byte(kv.Key))
		e.bytes([]byte(kv.Value))
		e.int(kv.CreateRevision)
		e.int(kv.ModRevision)
		e.int(kv.Version)
		e.int(kv.Lease)
	}
	e.uint(uint64(len(st.Leases)))
	for _, id := range st.Leases {
		e.int(id)
	}
	e.bytes(synth)
	if e.err != nil {
		return e.err
	}
	// The record's bytes and the disks' pages came from the runner's
	// disk: a failed read of it gave zeros, which no image may hold.
	if err := img.src.pagesErr(); err != nil {
		return err
	}
	return e.w.Flush()
}

// savedSynth is a synthStore as an image's file holds it. Its keys and
// values are bytes, which JSON keeps as they are.
type savedSynth struct {
	Rev    int64
	Seeds  []mvcc.Seed
	Leases map[int64]int64
}

// loadImage reads an image of size bytes that save wrote, from r. Its
// disks keep their pages in a store of their own on the runner's disk, as
// an aging run's do, past a cache of keepPages each, as they are read.
func loadImage(r io.ReaderAt, size int64) (*image, error) {
	d := &imageDecoder{r: bufio.NewReader(io.NewSectionReader(r, 0, size)), left: size}
	if string(d.bytes()) != imageMagic {
		return nil, fmt.Errorf("%w: no header", errImageFile)
	}
	img := &image{at: time.Unix(0, d.int()), src: &Sim{}}
	pages, err := simrun.NewHostPages()
	if err != nil {
		return nil, fmt.Errorf("%w: the disks' pages: %w", errRunnerDisk, err)
	}
	chunks, err := simrun.NewHostPages()
	if err != nil {
		return nil, fmt.Errorf("%w: the history's chunks: %w", errRunnerDisk, err)
	}
	img.src.pages, img.src.chunks = &keptPages{pages: pages}, &keptPages{pages: chunks}
	ca, cert := d.bytes(), d.bytes()
	net := &img.src.net
	nhosts := d.count(64)
	for range nhosts {
		net.hosts = append(net.hosts, string(d.bytes()))
	}
	net.expires, net.valid = time.Unix(0, d.int()), time.Duration(d.int())
	ndisks := d.count(16)
	for range ndisks {
		img.ids = append(img.ids, d.uint())
		img.disks = append(img.disks, d.disk(img.src.pages))
	}
	img.maxLogged, img.applied, img.ops = d.uint(), d.uint(), int(d.count(1<<40))
	l := &entryLog{from: d.uint(), data: arena{store: img.src.chunks}}
	nrecs := d.count(1 << 32)
	next := l.from
	for range nrecs {
		i, term, typ, known, hash, node := d.uint(), d.uint(), d.uint(), d.bool(), d.uint(), d.uint()
		data := d.bytes()
		if d.err != nil {
			break
		}
		// Indexes rise, and stay within the record's reach.
		if i < next || i-l.from > 1<<32 {
			d.fail(fmt.Errorf("entry %d after %d, the record from %d", i, next, l.from))
			break
		}
		next = i + 1
		l.put(i, logRec{term: term, typ: raftpb.EntryType(typ), data: l.data.add(data), known: known, hash: hash, node: node})
	}
	img.log = l
	img.ckpts = map[uint64]ckptRec{}
	nckpts := d.count(1 << 32)
	for range nckpts {
		i := d.uint()
		img.ckpts[i] = ckptRec{sum: d.uint(), node: d.uint()}
	}
	for _, m := range []*map[int64]int64{&img.expiredAt, &img.revokedAt} {
		*m = map[int64]int64{}
		n := d.count(1 << 32)
		for range n {
			id := d.int()
			(*m)[id] = d.int()
		}
	}
	img.start = &history.Start{Rev: d.int()}
	nkvs := d.count(1 << 32)
	for range nkvs {
		kv := history.KeyValue{Key: string(d.bytes()), Value: string(d.bytes())}
		kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease = d.int(), d.int(), d.int(), d.int()
		img.start.KVs = append(img.start.KVs, kv)
	}
	nleases := d.count(1 << 32)
	for range nleases {
		img.start.Leases = append(img.start.Leases, d.int())
	}
	synth := d.bytes()
	if d.err != nil {
		return nil, d.err
	}
	if _, err := d.r.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: bytes past its end", errImageFile)
	}
	if net.signer, err = testcert.LoadCA(ca); err != nil {
		return nil, err
	}
	net.ca = net.signer.Pool
	if net.cert, err = testcert.LoadCertificate(cert); err != nil {
		return nil, err
	}
	if len(synth) > 0 {
		var s savedSynth
		if err := json.Unmarshal(synth, &s); err != nil {
			return nil, err
		}
		img.synth = &synthStore{rev: s.Rev, seeds: s.Seeds, leases: s.Leases}
	}
	if err := img.src.pagesErr(); err != nil {
		return nil, err
	}
	return img, nil
}

// countWriter counts what is written to it, and keeps nothing.
type countWriter int64

func (c *countWriter) Write(p []byte) (int, error) {
	*c += countWriter(len(p))
	return len(p), nil
}

// imageEncoder writes an image's fields, and keeps the first error.
type imageEncoder struct {
	w   *bufio.Writer
	b   []byte
	err error
}

func (e *imageEncoder) write(b []byte) {
	if e.err == nil {
		_, e.err = e.w.Write(b)
	}
}

func (e *imageEncoder) uint(v uint64) { e.write(binary.AppendUvarint(e.b[:0], v)) }
func (e *imageEncoder) int(v int64)   { e.write(binary.AppendVarint(e.b[:0], v)) }

func (e *imageEncoder) bool(v bool) {
	if v {
		e.uint(1)
	} else {
		e.uint(0)
	}
}

func (e *imageEncoder) bytes(p []byte) {
	e.uint(uint64(len(p)))
	e.write(p)
}

// imageDecoder reads an image's fields, and keeps the first error: after
// it, every read gives zero. left is how many bytes of the file are not
// read yet, which bounds every length it reads.
type imageDecoder struct {
	r    *bufio.Reader
	left int64
	err  error
}

func (d *imageDecoder) fail(err error) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: %w", errImageFile, err)
	}
}

// ReadByte is d's reader's, counted against left.
func (d *imageDecoder) ReadByte() (byte, error) {
	c, err := d.r.ReadByte()
	if err == nil {
		d.left--
	}
	return c, err
}

func (d *imageDecoder) uint() uint64 {
	if d.err != nil {
		return 0
	}
	v, err := binary.ReadUvarint(d)
	if err != nil {
		d.fail(err)
	}
	return v
}

func (d *imageDecoder) int() int64 {
	if d.err != nil {
		return 0
	}
	v, err := binary.ReadVarint(d)
	if err != nil {
		d.fail(err)
	}
	return v
}

func (d *imageDecoder) bool() bool { return d.uint() != 0 }

// count reads a count of at most most, and gives 0 after an error.
func (d *imageDecoder) count(most uint64) uint64 {
	n := d.uint()
	if n > most {
		d.fail(fmt.Errorf("a count of %d", n))
		return 0
	}
	return n
}

// length reads the length of a field, which the file must still hold.
func (d *imageDecoder) length() int64 {
	n := d.uint()
	if d.err == nil && n > uint64(d.left) {
		d.fail(fmt.Errorf("a field of %d bytes, with %d left", n, d.left))
		return 0
	}
	return int64(n)
}

func (d *imageDecoder) bytes() []byte {
	n := d.length()
	if d.err != nil {
		return nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(d.r, b); err != nil {
		d.fail(err)
		return nil
	}
	d.left -= n
	return b
}

// disk reads a disk that simfs.Disk.Save wrote, its pages into pages as
// they come.
func (d *imageDecoder) disk(pages simfs.Store) *simfs.Disk {
	n := d.length()
	if d.err != nil {
		return nil
	}
	lr := &io.LimitedReader{R: d.r, N: n}
	disk, err := simfs.Load(lr, pages, keepPages)
	if err != nil {
		d.fail(err)
		return nil
	}
	if lr.N != 0 {
		d.fail(fmt.Errorf("a disk left %d of its %d bytes", lr.N, n))
		return nil
	}
	d.left -= n
	return disk
}
