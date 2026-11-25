package sim

import (
	"bufio"
	"bytes"
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
// process, which no earlier fork or aging run left anything in, alone or
// in a campaign's sequence. The file holds all a fork takes of its image:
// when it was taken, its certificates, its disks as a power loss leaves
// them (simfs.Disk.Save), its members' IDs, the record of what its nodes
// committed, and its store (history.Start), or a synthetic image's.

// imageMagic starts an image's file.
const imageMagic = "tigerstore sim image 1\n"

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
	var disks [][]byte
	for _, d := range img.disks {
		var b bytes.Buffer
		if err := d.Save(&b); err != nil {
			return err
		}
		disks = append(disks, b.Bytes())
	}
	start, err := json.Marshal(img.start)
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
	e.uint(uint64(len(disks)))
	for i, d := range disks {
		e.uint(img.ids[i])
		e.bytes(d)
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
	e.bytes(start)
	e.bytes(synth)
	if e.err != nil {
		return e.err
	}
	return e.w.Flush()
}

// savedSynth is a synthStore as an image's file holds it.
type savedSynth struct {
	Rev    int64
	Seeds  []mvcc.Seed
	Leases map[int64]int64
}

// loadImage reads an image that save wrote. Its disks keep their pages in
// a store of their own on the runner's disk, as an aging run's do.
func loadImage(r io.Reader) (*image, error) {
	d := &imageDecoder{r: bufio.NewReader(r)}
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
	nhosts := d.uint()
	for range min(nhosts, 64) {
		net.hosts = append(net.hosts, string(d.bytes()))
	}
	net.expires, net.valid = time.Unix(0, d.int()), time.Duration(d.int())
	ndisks := d.uint()
	for range min(ndisks, 16) {
		img.ids = append(img.ids, d.uint())
		disk, err := simfs.Load(bytes.NewReader(d.bytes()))
		if err != nil && d.err == nil {
			d.err = err
		}
		if disk != nil {
			disk.KeepIn(img.src.pages, keepPages)
		}
		img.disks = append(img.disks, disk)
	}
	img.maxLogged, img.applied, img.ops = d.uint(), d.uint(), int(d.uint())
	l := &entryLog{from: d.uint(), data: arena{store: img.src.chunks}}
	nrecs := d.uint()
	for range nrecs {
		if d.err != nil {
			break
		}
		i, term, typ, known, hash, node := d.uint(), d.uint(), d.uint(), d.bool(), d.uint(), d.uint()
		data := d.bytes()
		l.put(i, logRec{term: term, typ: raftpb.EntryType(typ), data: l.data.add(data), known: known, hash: hash, node: node})
	}
	img.log = l
	img.ckpts = map[uint64]ckptRec{}
	nckpts := d.uint()
	for range nckpts {
		if d.err != nil {
			break
		}
		i := d.uint()
		img.ckpts[i] = ckptRec{sum: d.uint(), node: d.uint()}
	}
	for _, m := range []*map[int64]int64{&img.expiredAt, &img.revokedAt} {
		*m = map[int64]int64{}
		n := d.uint()
		for range n {
			if d.err != nil {
				break
			}
			id := d.int()
			(*m)[id] = d.int()
		}
	}
	start, synth := d.bytes(), d.bytes()
	if d.err != nil {
		return nil, d.err
	}
	if nhosts > 64 || ndisks > 16 {
		return nil, fmt.Errorf("%w: %d hosts and %d disks", errImageFile, nhosts, ndisks)
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
	img.start = &history.Start{}
	if err := json.Unmarshal(start, img.start); err != nil {
		return nil, err
	}
	if len(synth) > 0 {
		var s savedSynth
		if err := json.Unmarshal(synth, &s); err != nil {
			return nil, err
		}
		img.synth = &synthStore{rev: s.Rev, seeds: s.Seeds, leases: s.Leases}
	}
	return img, nil
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
// it, every read gives zero.
type imageDecoder struct {
	r   *bufio.Reader
	err error
}

func (d *imageDecoder) fail(err error) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: %w", errImageFile, err)
	}
}

func (d *imageDecoder) uint() uint64 {
	if d.err != nil {
		return 0
	}
	v, err := binary.ReadUvarint(d.r)
	if err != nil {
		d.fail(err)
	}
	return v
}

func (d *imageDecoder) int() int64 {
	if d.err != nil {
		return 0
	}
	v, err := binary.ReadVarint(d.r)
	if err != nil {
		d.fail(err)
	}
	return v
}

func (d *imageDecoder) bool() bool { return d.uint() != 0 }

func (d *imageDecoder) bytes() []byte {
	n := d.uint()
	if d.err != nil {
		return nil
	}
	if n > 1<<40 {
		d.fail(fmt.Errorf("a field of %d bytes", n))
		return nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(d.r, b); err != nil {
		d.fail(err)
		return nil
	}
	return b
}
