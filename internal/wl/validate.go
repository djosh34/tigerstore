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
		return checkTxn(msg, maxTxnOps)
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

// checkBranch checks each operation of a Txn branch, and that no two of
// its writes touch the same key.
func checkBranch(msg []byte, branch, maxOps int) error {
	var puts [][]byte
	var dels [][2][]byte
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
			var r putReq
			_ = parsePut(op, &r)
			puts = append(puts, r.key)
		case 3:
			if err := checkDel(op); err != nil {
				return err
			}
			var r delReq
			_ = parseDel(op, &r)
			dels = append(dels, [2][]byte{r.key, r.end})
		case 4:
			if err := checkTxn(op, maxOps); err != nil {
				return err
			}
		default:
			return invalid(msgKeyNotFound)
		}
	}
	for i, k := range puts {
		for _, q := range puts[:i] {
			if bytes.Equal(q, k) {
				return invalid(msgDuplicateKey)
			}
		}
		for _, d := range dels {
			if inRange(k, d[0], d[1]) {
				return invalid(msgDuplicateKey)
			}
		}
	}
	return nil
}

// inRange reports whether key lies in [start, end), etcd's range of one
// key when end is empty and of every key from start when end is "\x00".
func inRange(key, start, end []byte) bool {
	switch {
	case len(end) == 0:
		return bytes.Equal(key, start)
	case len(end) == 1 && end[0] == 0:
		return bytes.Compare(key, start) >= 0
	}
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
}
