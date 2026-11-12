package loop

// Buffers lends buffers of one size to the loop's connections, so that a
// connection holds one only while it has bytes in it: an idle connection
// holds none. The loop alone uses it, so it takes no lock.
type Buffers struct {
	size int
	free [][]byte
}

// maxFree is how many returned buffers Buffers keeps for the next Get.
const maxFree = 64

// NewBuffers returns buffers of size bytes.
func NewBuffers(size int) *Buffers { return &Buffers{size: size} }

// Size is the size of the buffers.
func (p *Buffers) Size() int { return p.size }

// Get returns a buffer of the pool's size.
func (p *Buffers) Get() []byte {
	if n := len(p.free); n > 0 {
		b := p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
		return b[:p.size]
	}
	return make([]byte, p.size)
}

// Put returns b, which then must not be used; one of another size goes to
// the garbage collector.
func (p *Buffers) Put(b []byte) {
	if cap(b) == p.size && len(p.free) < maxFree {
		p.free = append(p.free, b[:p.size])
	}
}
