package simfs

// Memory is an exact count of byte-slice lengths and capacities reachable
// through a disk's directory. It excludes allocator rounding, maps, open
// unlinked handles and the structs; heap profiles measure those separately.
type Memory struct {
	Files, Live, DataCap, Durable, DurableCap, Pending, PendingCap int
	Written                                                        uint64
}

func (d *Disk) Memory() Memory {
	m := Memory{Written: d.Written}
	seen := map[*file]bool{}
	for _, f := range d.names {
		m.Files++
		m.Live += len(f.data)
	}
	add := func(f *file) {
		if seen[f] {
			return
		}
		seen[f] = true
		m.DataCap += cap(f.data)
		m.Durable += len(f.durable)
		m.DurableCap += cap(f.durable)
		for _, w := range f.pending {
			m.Pending += len(w.p)
			m.PendingCap += cap(w.p)
		}
	}
	for _, f := range d.names {
		add(f)
	}
	for _, f := range d.durable {
		add(f)
	}
	for _, o := range d.dirOps {
		add(o.f)
	}
	return m
}
