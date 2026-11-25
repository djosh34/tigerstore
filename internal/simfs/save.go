package simfs

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
)

// A disk's durable state, what a power loss keeps, can be saved and loaded
// again in another process: the simulator saves an aged image's disks once
// and starts each fork in a run of its own from them (#794). Saved are the
// durable directories, every durable file with its id and its size, the
// pages that hold bytes, and the count of files made, so that the loaded
// disk names its next file as the saved one would. Pages that were never
// written stay unwritten, so a file of mostly holes, as a formatted log
// ring is, takes only its written pages.

// saveMagic starts a saved disk.
const saveMagic = "simfs disk 1\n"

// ErrSaved means that a saved disk does not parse.
var ErrSaved = errors.New("simfs: malformed saved disk")

// Save writes the durable state of d to w: the disk a power loss would
// leave, as Durable has it.
func (d *Disk) Save(w io.Writer) error {
	durable, dirs := d.durableView()
	bw := bufio.NewWriter(w)
	var b []byte
	b = append(b, saveMagic...)
	b = binary.AppendUvarint(b, uint64(d.files))
	ds := slices.Sorted(func(yield func(string) bool) {
		for name, ok := range dirs {
			if ok && !yield(name) {
				return
			}
		}
	})
	b = binary.AppendUvarint(b, uint64(len(ds)))
	for _, name := range ds {
		b = appendString(b, name)
	}
	names := sortedNames(durable)
	b = binary.AppendUvarint(b, uint64(len(names)))
	if _, err := bw.Write(b); err != nil {
		return err
	}
	var scratch []byte
	for _, name := range names {
		f := durable[name]
		b = appendString(b[:0], name)
		b = binary.AppendUvarint(b, uint64(f.id))
		b = binary.AppendUvarint(b, uint64(f.durable.size))
		var held []int
		for i, pg := range f.durable.blocks {
			if pg != nil {
				held = append(held, i)
			}
		}
		b = binary.AppendUvarint(b, uint64(len(held)))
		if _, err := bw.Write(b); err != nil {
			return err
		}
		for _, i := range held {
			b = binary.AppendUvarint(b[:0], uint64(i))
			if _, err := bw.Write(b); err != nil {
				return err
			}
			page := f.durable.get(i, &scratch)
			if page == nil {
				page = make([]byte, PageBytes)
			}
			if _, err := bw.Write(page); err != nil {
				return err
			}
		}
	}
	if d.st != nil && d.st.err != nil {
		return fmt.Errorf("simfs: reading the disk's pages to save them: %w", d.st.err)
	}
	return bw.Flush()
}

// Load reads a disk that Save wrote: one with those durable files and
// directories, nothing pending, and no faults, Store or capacity set.
func Load(r io.Reader) (*Disk, error) {
	br := bufio.NewReader(r)
	magic := make([]byte, len(saveMagic))
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != saveMagic {
		return nil, fmt.Errorf("%w: no header", ErrSaved)
	}
	d := New()
	files, err := readUvarint(br)
	if err != nil {
		return nil, err
	}
	d.files = int(files)
	ndirs, err := readUvarint(br)
	if err != nil {
		return nil, err
	}
	for range ndirs {
		name, err := readString(br)
		if err != nil {
			return nil, err
		}
		d.dirs[name], d.durableDirs[name] = true, true
	}
	nfiles, err := readUvarint(br)
	if err != nil {
		return nil, err
	}
	for range nfiles {
		name, err := readString(br)
		if err != nil {
			return nil, err
		}
		var vals [3]uint64
		for i := range vals {
			if vals[i], err = readUvarint(br); err != nil {
				return nil, err
			}
		}
		id, size, held := vals[0], vals[1], vals[2]
		if size > 1<<50 || held > uint64(pageCount(int(size))) {
			return nil, fmt.Errorf("%w: file %q of %d bytes with %d pages", ErrSaved, name, size, held)
		}
		f := &file{id: int(id)}
		f.durable.size = int(size)
		last := -1
		for range held {
			i, err := readUvarint(br)
			if err != nil {
				return nil, err
			}
			if int(i) <= last || int(i) >= pageCount(int(size)) {
				return nil, fmt.Errorf("%w: file %q has page %d after %d", ErrSaved, name, i, last)
			}
			last = int(i)
			b := new(block)
			if _, err := io.ReadFull(br, b[:]); err != nil {
				return nil, fmt.Errorf("%w: file %q page %d: %w", ErrSaved, name, i, err)
			}
			f.durable.set(int(i), b)
		}
		f.reset()
		d.names[name], d.durable[name] = f, f
	}
	if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: bytes past its end", ErrSaved)
	}
	return d, nil
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func readUvarint(r *bufio.Reader) (uint64, error) {
	v, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrSaved, err)
	}
	return v, nil
}

func readString(r *bufio.Reader) (string, error) {
	n, err := readUvarint(r)
	if err != nil {
		return "", err
	}
	if n > 1<<16 {
		return "", fmt.Errorf("%w: a name of %d bytes", ErrSaved, n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", fmt.Errorf("%w: %w", ErrSaved, err)
	}
	return string(b), nil
}
