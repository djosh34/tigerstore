package fs

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"slices"
	"testing"

	"github.com/jellevandenhooff/gosim/internal/simulation/syscallabi"
)

func TestMakeChunks(t *testing.T) {
	data := make([]byte, chunkSize*8)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	interesting := []int{
		0, 10, chunkSize - 10, chunkSize, chunkSize + 10,
		2*chunkSize - 10, 2 * chunkSize, 2*chunkSize + 10,
		7*chunkSize - 10, 7 * chunkSize, 7*chunkSize + 10,
		len(data),
	}

	for _, start := range interesting {
		for _, end := range interesting {
			if start > end {
				continue
			}
			t.Run(fmt.Sprintf("%d-%d", start, end), func(t *testing.T) {
				view := syscallabi.ByteSliceView{Ptr: data[start:end]}
				chunks := makeChunks(start, view)
				if count := requiredCount(start%chunkSize, end-start); count != len(chunks) {
					t.Errorf("calculated count %d but got %d", count, len(chunks))
				}
				if start == end {
					return
				}
				chunkPos := start % chunkSize
				if chunkPos != 0 {
					n := min(end-start, chunkSize-chunkPos)
					if !bytes.Equal(chunks[0].data[:n], data[start:start+n]) {
						t.Errorf("problem for %d-%d", start, start+n)
					}
					chunks = chunks[1:]
					start += n
				}
				for start < end {
					n := min(end-start, chunkSize)
					if !bytes.Equal(chunks[0].data[:n], data[start:start+n]) {
						t.Errorf("problem for %d-%d", start, start+n)
					}
					chunks = chunks[1:]
					start += n
				}
			})
		}
	}
}

// TODO: have another wrapper around chunkedFile that does explicit []byte
// reads/writes so we can check those manually

type checker struct {
	f *chunkedFile
	b []byte
}

func newChecker() *checker {
	return &checker{
		f: &chunkedFile{},
	}
}

func (c *checker) resize(newN int) {
	oldN := len(c.b)
	if newN > oldN {
		newB := append(c.b, make([]byte, newN-oldN)...)
		clear(newB[oldN:newN])
		c.b = newB
	} else {
		c.b = c.b[:newN]
	}
	if len(c.b) != newN {
		panic("help")
	}

	c.f.Resize(newN)
}

func (c *checker) write(t *testing.T, from, to int) {
	t.Helper()

	if _, err := rand.Read(c.b[from:to]); err != nil {
		t.Fatal(err)
	}

	chunks := makeChunks(from, syscallabi.ByteSliceView{Ptr: c.b[from:to]})
	c.f.Write(from, to-from, chunks)
	for _, chunk := range chunks {
		chunk.decRef()
	}
}

func (c *checker) read(t *testing.T, from, to int) {
	t.Helper()

	buf := make([]byte, to-from)
	c.f.Read(from, syscallabi.ByteSliceView{Ptr: buf})
	if !bytes.Equal(buf, c.b[from:to]) {
		firstBad := 0
		for buf[firstBad] == c.b[from+firstBad] {
			firstBad++
		}
		lastBad := (to - from)
		for buf[lastBad-1] == c.b[from+lastBad-1] {
			lastBad--
		}
		t.Errorf("read %d-%d: bad data %d-%d", from, to, from+firstBad, from+lastBad)
	}
}

func TestFileRead(t *testing.T) {
	data := make([]byte, chunkSize*8)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	interesting := []int{
		0, 10, chunkSize - 10, chunkSize, chunkSize + 10,
		2*chunkSize - 10, 2 * chunkSize, 2*chunkSize + 10,
		7*chunkSize - 10, 7 * chunkSize, 7*chunkSize + 10,
		len(data),
	}

	check := func(size int, file *chunkedFile) {
		t.Helper()

		c := &checker{
			f: file,
			b: data[:size],
		}

		for _, start := range interesting {
			for _, end := range interesting {
				if start > end || end > size {
					continue
				}

				c.read(t, start, end)
			}
		}
	}

	for _, size := range interesting {
		chunks := makeChunks(0, syscallabi.ByteSliceView{Ptr: data})
		file := &chunkedFile{size: size, chunks: chunks}
		check(size, file)
	}

	// for the second iteration, zero out a chunk of the input and put a nil chunk there
	copy(data[chunkSize:chunkSize*2], make([]byte, chunkSize))

	for _, size := range interesting {
		chunks := makeChunks(0, syscallabi.ByteSliceView{Ptr: data})
		if len(chunks) > 2 {
			chunks[1] = nil
		}
		file := &chunkedFile{size: size, chunks: chunks}
		check(size, file)
	}
}

func (f *checker) clone() *checker {
	return &checker{
		f: f.f.Clone(),
		b: slices.Clone(f.b),
	}
}

func (f *checker) free() {
	f.f.Free()
	f.f = nil
	f.b = nil
}

func TestFileWrite(t *testing.T) {
	f := newChecker()
	f.resize(chunkSize * 3)
	f.read(t, 0, 2*chunkSize)
	f.read(t, chunkSize-10, chunkSize+10)
	f.read(t, chunkSize-10, 2*chunkSize+10)
	f.write(t, 10, chunkSize-10)
	f.read(t, 10, chunkSize-10)
	f.read(t, 0, chunkSize)
	f.read(t, 0, 2*chunkSize)
	f.read(t, 0, chunkSize*3)
	f.write(t, 0, chunkSize*3)
	f.read(t, 0, chunkSize*3)
	f.resize(chunkSize*3 - 10)
	f.resize(chunkSize * 3)
	f.read(t, 0, chunkSize*3)

	// XXX: zero-sized writes?
	f.write(t, 10, 10)
	f.write(t, chunkSize, chunkSize)
	f.write(t, 0, 0)

	f.resize(chunkSize*5 - 10)
	f.read(t, 0, chunkSize*5-10)
	f.write(t, 0, chunkSize*5-10)
	f.read(t, 0, chunkSize*5-10)

	t.Logf("%d %p", f.f.chunks[0].refs, f.f.chunks[0])
	g := f.clone()
	t.Logf("%d %p", f.f.chunks[0].refs, f.f.chunks[0])
	t.Logf("%d %p", g.f.chunks[0].refs, g.f.chunks[0])
	g.read(t, 0, chunkSize*5-10)
	g.write(t, 10, 20)
	g.read(t, 0, chunkSize)
	f.read(t, 0, chunkSize)

	g.free()

	f.read(t, 0, chunkSize)
	f.resize(10 * chunkSize)
	g = f.clone()
	g.write(t, 0, chunkSize*10)
	f.read(t, 0, 10*chunkSize)
	g.read(t, 0, 10*chunkSize)

	// TODO: check ref count?
	// TODO: check for nil?

	// TODO: file write releasing empty chunk. but how? poll pool? maybe check
	// allocs for a write cycle? or check that a chunk is reused?
}
