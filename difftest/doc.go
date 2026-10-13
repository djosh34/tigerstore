// Package difftest holds sequential differential tests against etcd v3.7.2.
//
// The tests are behind the difftest build tag and talk to one endpoint over
// the raw etcd v3 gRPC API. scripts/difftest.sh starts a fresh server and
// runs them:
//
//	scripts/difftest.sh etcd          single-node etcd v3.7.2, ETCD is its binary
//	scripts/difftest.sh tigerstore    3-node tigerstore built from this checkout
//
// testdata/etcd-v3.7.2.json holds etcd v3.7.2's answers. The tests compare the
// endpoint's answers with it, step by step. `scripts/difftest.sh etcd record`
// writes the file again and refuses an endpoint that is not etcd 3.7.2.
//
// Two lists in boundaries_test.go say where tigerstore may differ. stricter
// holds the watch steps where etcd sends an update without its previous
// value, which Kubernetes rejects; tigerstore must not copy that. expectedFail
// holds the steps tigerstore does not match yet, each with an issue.
// testdata/tigerstore-known.json holds tigerstore's answer to each of them,
// and a listed step must give exactly that answer. `scripts/difftest.sh
// tigerstore record` writes that file again. A listed step that starts to
// match etcd fails the test until it is removed from the list.
//
// TIGERSTORE_DIFF_SERVER (etcd or tigerstore) and TIGERSTORE_DIFF_ENDPOINT are
// required. For a TLS endpoint, set TIGERSTORE_DIFF_CA, TIGERSTORE_DIFF_CERT
// and TIGERSTORE_DIFF_KEY to PEM files.
package difftest
