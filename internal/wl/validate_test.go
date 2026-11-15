package wl

import (
	"errors"
	"testing"
)

func putOp(key, value string) []byte {
	return appendBytesField(appendBytesField(nil, 1, []byte(key)), 2, []byte(value))
}

func delOp(key, end string) []byte {
	b := appendBytesField(nil, 1, []byte(key))
	if end != "" {
		b = appendBytesField(b, 2, []byte(end))
	}
	return b
}

// txn builds a TxnRequest whose success branch has ops, each a field
// number of RequestOp and its request.
func txn(ops ...[]byte) []byte {
	var b []byte
	for _, op := range ops {
		b = appendBytesField(b, 2, op)
	}
	return b
}

func reqOp(num int, req []byte) []byte { return appendBytesField(nil, num, req) }

func rejected(err error) string {
	if re, ok := errors.AsType[*requestError](err); ok {
		return re.msg
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func TestRequestChecks(t *testing.T) {
	ignoreValue := appendVarintField(putOp("/k", "v"), 5, 1)
	ignoreLease := appendVarintField(appendVarintField(putOp("/k", ""), 3, 7), 6, 1)
	var many [][]byte
	for range maxTxnOps + 1 {
		many = append(many, reqOp(1, delOp("/r", "")))
	}
	for _, c := range []struct {
		name string
		kind uint8
		msg  []byte
		want string
	}{
		{"put", kPut, putOp("/k", "v"), ""},
		{"put without a key", kPut, putOp("", "v"), msgEmptyKey},
		{"put of a value it ignores", kPut, ignoreValue, msgValueProvided},
		{"put of a lease it ignores", kPut, ignoreLease, msgLeaseProvided},
		{"delete without a key", kDel, delOp("", ""), msgEmptyKey},
		{"txn", kTxn, txn(reqOp(2, putOp("/a", "1")), reqOp(2, putOp("/b", "2")), reqOp(3, delOp("/c", ""))), ""},
		{"txn putting a key twice", kTxn, txn(reqOp(2, putOp("/a", "1")), reqOp(2, putOp("/a", "2"))), msgDuplicateKey},
		{"txn putting into its delete", kTxn, txn(reqOp(3, delOp("/a", "/b")), reqOp(2, putOp("/a/x", "1"))), msgDuplicateKey},
		{"txn with an empty op", kTxn, txn([]byte{}), msgKeyNotFound},
		{"txn with too many ops", kTxn, txn(many...), msgTooManyOps},
		{"nested txn putting without a key", kTxn, txn(reqOp(4, txn(reqOp(2, putOp("", "v"))))), msgEmptyKey},
		{"request too large", kPut, putOp("/k", string(make([]byte, maxRequestBytes))), msgTooLarge},
	} {
		if got := rejected(checkRequest(c.kind, c.msg)); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}
