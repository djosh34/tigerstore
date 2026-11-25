package sim

import (
	"bytes"
	"errors"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// An image saved to its file loads as it was: the same disks, members,
// record, store and certificates, and saved again it gives the same
// bytes, which a fork's replay checks by their SHA-256 (#794). A file cut
// short does not load.
func TestAnImageLoadsAsItWasSaved(t *testing.T) {
	// A restore onto a simulated disk needs wl's seam, which a plain go
	// test leaves out: Here skips there, as every simulator test does.
	simrun.Here(t)
	cfg := synthConfig(5, 40)
	spec := syntheticSpec(5, cfg.voters(), 40)
	var img *image
	var err error
	simrun.BubbleFor(t, time.Hour, func(*testing.T) {
		var s *Sim
		if s, err = newSim(cfg); err == nil {
			img, err = s.restoreImage(spec)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	img.at = time.Unix(1700000000, 5)
	img.log.put(img.log.from+1, logRec{term: 7, typ: 0, data: img.log.data.add([]byte("entry")), known: true, hash: 99, node: spec.ids[0]})
	img.ckpts[img.log.from] = ckptRec{sum: 42, node: spec.ids[len(spec.ids)-1]}
	img.expiredAt[7] = -5
	// A value that is no UTF-8, which JSON would have changed.
	img.start.KVs[0].Value = "\xff\xfe\x00v"
	var b bytes.Buffer
	if err := img.save(&b); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadImage(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}
	var again bytes.Buffer
	if err := loaded.save(&again); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again.Bytes(), b.Bytes()) {
		t.Fatal("a loaded image saves to other bytes")
	}
	if !loaded.at.Equal(img.at) || !slices.Equal(loaded.ids, img.ids) || loaded.maxLogged != img.maxLogged || loaded.applied != img.applied ||
		!maps.Equal(loaded.ckpts, img.ckpts) || !maps.Equal(loaded.expiredAt, img.expiredAt) || !reflect.DeepEqual(loaded.start, img.start) ||
		!reflect.DeepEqual(loaded.synth, img.synth) {
		t.Fatal("the loaded image's fields differ")
	}
	r, ok := loaded.log.get(img.log.from + 1)
	if !ok || r.term != 7 || string(loaded.log.bytes(r)) != "entry" || r.hash != 99 {
		t.Fatalf("the loaded record's entry is %+v", r)
	}
	for i, d := range loaded.disks {
		if !slices.Equal(d.DurableNames(), img.disks[i].DurableNames()) {
			t.Fatalf("disk %d holds %v, want %v", i, d.DurableNames(), img.disks[i].DurableNames())
		}
		for _, name := range d.DurableNames() {
			if !bytes.Equal(d.DurableBytes(name), img.disks[i].DurableBytes(name)) {
				t.Fatalf("disk %d's %s differs", i, name)
			}
		}
	}
	if !bytes.Equal(loaded.src.net.cert.Certificate[0], img.src.net.cert.Certificate[0]) || !loaded.src.net.expires.Equal(img.src.net.expires) {
		t.Fatal("the loaded certificate differs")
	}
	for _, cut := range []int{0, 10, b.Len() / 2, b.Len() - 1} {
		if _, err := loadImage(bytes.NewReader(b.Bytes()[:cut]), int64(cut)); err == nil {
			t.Fatalf("an image cut at %d of %d bytes loaded", cut, b.Len())
		}
	}
	if _, err := loadImage(bytes.NewReader(append(b.Bytes(), 0)), int64(b.Len()+1)); !errors.Is(err, errImageFile) {
		t.Fatalf("an image with a byte past its end gave %v", err)
	}
}
