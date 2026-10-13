// Package difftest holds sequential differential tests against etcd v3.7.2.
//
// The tests are behind the difftest build tag and talk to one endpoint over
// the raw etcd v3 gRPC API:
//
//	TIGERSTORE_DIFF_ENDPOINT=127.0.0.1:23790 go test -tags difftest ./difftest
//
// testdata/etcd-v3.7.2.json holds etcd v3.7.2's answers. The tests compare the
// endpoint's answers with it, so the same tests run against tigerstore. To
// record the file again, start a fresh single-node etcd v3.7.2 as the ci
// workflow does and add -record. Recording refuses an endpoint that is not
// etcd 3.7.2.
//
// For a TLS endpoint, set TIGERSTORE_DIFF_CA, TIGERSTORE_DIFF_CERT and
// TIGERSTORE_DIFF_KEY to PEM files.
package difftest
