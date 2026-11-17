package wl

import "bytes"

// Requests are checked as they come in, before they are proposed, as
// etcd's server checks them: a request the apply would reject never
// reaches the log, and no Txn stops halfway through its apply.

const (
	maxTxnOps       = 128
	maxRequestBytes = 1536 << 10 // etcd's --max-request-bytes

	msgEmptyKey      = "etcdserver: key is not provided"
	msgValueProvided = "etcdserver: value is provided"
	msgLeaseProvided = "etcdserver: lease is provided"
	msgTooManyOps    = "etcdserver: too many operations in txn request"
	msgDuplicateKey  = "etcdserver: duplicate key given in txn request"
	msgKeyNotFound   = "etcdserver: key not found"
	msgTooLarge      = "etcdserver: request is too large"
	msgSortOption    = "etcdserver: invalid sort option"
)

func invalid(msg string) error { return requestErr(codeInvalidArgument, msg) }

// raftRequestBytes is what etcd's InternalRaftRequest adds around a
// write of n bytes: its header, field 100 with a request ID of 10 bytes,
// etcd's longest, and the write's tag, one byte, and length.
func raftRequestBytes(n int) int { return 2 + 1 + 1 + 10 + 1 + sizeVarint(uint64(n)) }

// identityBytes is what a client's name adds to etcd's InternalRaftRequest:
// with --client-cert-auth, etcd puts the name of the client's certificate
// in every write's header as its user, with auth revision 1 (#99).
func identityBytes(cn string) int {
	if cn == "" {
		return 0
	}
	return sizeBytesField(2, len(cn)) + sizeVarintField(3, 1)
}

// checkRequest checks a write or a Txn of kind before it is proposed. As
// etcd's server, it limits the size of the request as etcd would
// replicate it, with identity, what the client's name adds.
func checkRequest(kind uint8, msg []byte, identity int) error {
	if raftRequestBytes(len(msg))+len(msg)+identity > maxRequestBytes {
		return invalid(msgTooLarge)
	}
	switch kind {
	case kPut:
		return checkPut(msg)
	case kDel:
		return checkDel(msg)
	case kTxn:
		if err := checkTxn(msg, maxTxnOps); err != nil {
			return err
		}
		for branch := 2; branch <= 3; branch++ {
			if err := intervals(msg, branch); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func checkRange(r *rangeReq, msg []byte) error {
	if len(r.key) == 0 {
		return invalid(msgEmptyKey)
	}
	for m := msg; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		if (num == 5 && v > 2) || (num == 6 && v > 4) { // sort order, sort target
			return invalid(msgSortOption)
		}
		m = rest
	}
	return nil
}

// checkRangeStream is etcd's checkRangeStreamRequest: a RangeStream
// neither sorts nor filters.
func checkRangeStream(r *rangeReq) error {
	if !r.defaultOrder() {
		return requestErr(codeUnimplemented, "RangeStream does not support custom sort orders")
	}
	if r.filters() {
		return requestErr(codeUnimplemented, "RangeStream does not support revision filters")
	}
	return nil
}

func checkPut(msg []byte) error {
	var r putReq
	if err := parsePut(msg, &r); err != nil {
		return badRequest(err)
	}
	switch {
	case len(r.key) == 0:
		return invalid(msgEmptyKey)
	case r.ignoreValue && len(r.value) != 0:
		return invalid(msgValueProvided)
	case r.ignoreLease && r.lease != 0:
		return invalid(msgLeaseProvided)
	}
	return nil
}

func checkDel(msg []byte) error {
	var r delReq
	if err := parseDel(msg, &r); err != nil {
		return badRequest(err)
	}
	if len(r.key) == 0 {
		return invalid(msgEmptyKey)
	}
	return nil
}

// checkTxn checks a Txn and the Txns inside it, which together may have
// at most maxOps operations in a list.
func checkTxn(msg []byte, maxOps int) error {
	var counts [4]int
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		m = rest
		if num < 1 || num > 3 {
			continue
		}
		counts[num]++
		if num == 1 {
			var c compare
			if err := parseCompare(p, &c); err != nil {
				return badRequest(err)
			}
			if len(c.key) == 0 {
				return invalid(msgEmptyKey)
			}
		}
	}
	opc := max(counts[1], counts[2], counts[3])
	if opc > maxOps {
		return invalid(msgTooManyOps)
	}
	for branch := 2; branch <= 3; branch++ {
		if err := checkBranch(msg, branch, maxOps-opc); err != nil {
			return err
		}
	}
	return nil
}

// checkBranch checks each operation of a Txn branch.
func checkBranch(msg []byte, branch, maxOps int) error {
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		m = rest
		if num != branch {
			continue
		}
		onum, _, _, op, _, err := field(p)
		if len(p) == 0 {
			return invalid(msgKeyNotFound)
		}
		if err != nil {
			return badRequest(err)
		}
		switch onum {
		case 1:
			var r rangeReq
			if err := parseRange(op, &r); err != nil {
				return badRequest(err)
			}
			if err := checkRange(&r, op); err != nil {
				return err
			}
		case 2:
			if err := checkPut(op); err != nil {
				return err
			}
		case 3:
			if err := checkDel(op); err != nil {
				return err
			}
		case 4:
			if err := checkTxn(op, maxOps); err != nil {
				return err
			}
		default:
			return invalid(msgKeyNotFound)
		}
	}
	return nil
}

// intervals is etcd's checkIntervals for a Txn's branch: no key is put
// twice, nor put and deleted, in the branch and the Txns nested in it,
// but for a key a nested Txn puts in both of its branches, of which only
// one runs. A branch with no nested Txn, a Kubernetes write's, is checked
// pair by pair without allocating; one with nested Txns takes etcd's
// steps (nestedIntervals).
func intervals(msg []byte, branch int) error {
	nested := false
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		m = rest
		if num == branch {
			if onum, _, _, _, _, err := field(p); err == nil && onum == 4 {
				nested = true
			}
		}
	}
	if nested {
		_, _, err := nestedIntervals(msg, branch)
		return err
	}
	// Each put against the other puts and the deletes of the branch.
	i := -1
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		m = rest
		if num != branch {
			continue
		}
		i++
		onum, _, _, op, _, err := field(p)
		if err != nil || onum != 2 {
			continue
		}
		var r putReq
		if err := parsePut(op, &r); err != nil {
			return badRequest(err)
		}
		j := -1
		for o := msg; len(o) > 0; {
			num, _, _, q, rest, err := field(o)
			if err != nil {
				return badRequest(err)
			}
			o = rest
			if num != branch {
				continue
			}
			if j++; j == i {
				continue
			}
			onum, _, _, op2, _, err := field(q)
			if err != nil {
				continue
			}
			switch onum {
			case 2:
				var r2 putReq
				if err := parsePut(op2, &r2); err != nil {
					return badRequest(err)
				}
				if bytes.Equal(r.key, r2.key) {
					return invalid(msgDuplicateKey)
				}
			case 3:
				var d delReq
				if err := parseDel(op2, &d); err != nil {
					return badRequest(err)
				}
				if overlaps(r.key, d.key, d.end) {
					return invalid(msgDuplicateKey)
				}
			}
		}
	}
	return nil
}

// nestedIntervals is etcd's checkIntervals itself, for a branch with
// nested Txns: it returns the keys the branch puts and the ranges it
// deletes.
func nestedIntervals(msg []byte, branch int) (map[string]bool, [][2][]byte, error) {
	type op struct {
		kind int
		body []byte
	}
	var ops []op
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return nil, nil, badRequest(err)
		}
		m = rest
		if num != branch {
			continue
		}
		onum, _, _, body, _, err := field(p)
		if err != nil {
			return nil, nil, badRequest(err)
		}
		ops = append(ops, op{onum, body})
	}
	var dels [][2][]byte
	deleted := func(k string) bool {
		for _, d := range dels {
			if overlaps([]byte(k), d[0], d[1]) {
				return true
			}
		}
		return false
	}
	for _, o := range ops {
		if o.kind == 3 {
			var r delReq
			if err := parseDel(o.body, &r); err != nil {
				return nil, nil, badRequest(err)
			}
			dels = append(dels, [2][]byte{r.key, r.end})
		}
	}
	puts := map[string]bool{}
	for _, o := range ops {
		if o.kind != 4 {
			continue
		}
		putsThen, delsThen, err := nestedIntervals(o.body, 2)
		if err != nil {
			return nil, nil, err
		}
		putsElse, delsElse, err := nestedIntervals(o.body, 3)
		if err != nil {
			return nil, nil, err
		}
		for k := range putsThen {
			if puts[k] || deleted(k) {
				return nil, nil, invalid(msgDuplicateKey)
			}
			puts[k] = true
		}
		for k := range putsElse {
			if (puts[k] && !putsThen[k]) || deleted(k) {
				return nil, nil, invalid(msgDuplicateKey)
			}
			puts[k] = true
		}
		dels = append(append(dels, delsThen...), delsElse...)
	}
	for _, o := range ops {
		if o.kind != 2 {
			continue
		}
		var r putReq
		if err := parsePut(o.body, &r); err != nil {
			return nil, nil, badRequest(err)
		}
		k := string(r.key)
		if puts[k] || deleted(k) {
			return nil, nil, invalid(msgDuplicateKey)
		}
		puts[k] = true
	}
	return puts, dels, nil
}

// overlaps is etcd's checkIntervals' test of a put's key against a
// delete's range: its interval tree takes the end as it is, so "\x00",
// which deletes every key from start, is an empty interval there and
// meets no put, as in etcd.
func overlaps(key, start, end []byte) bool {
	if len(end) == 0 {
		return bytes.Equal(key, start)
	}
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
}
