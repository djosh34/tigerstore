package wl

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"

	"github.com/djosh34/tigerstore/internal/metrics"
)

func TestMetricsNameCallsAsGRPCDoes(t *testing.T) {
	n := &node{st: &store{}, metrics: &nodeMetrics{}}
	put, txn, watch := &stream{kind: kPut}, &stream{kind: kTxn}, &stream{kind: kWatch}
	for _, s := range []*stream{put, put, txn, watch} {
		n.callStarted(s)
	}
	n.callEnded(put, 0)
	n.callEnded(put, 0)
	n.callEnded(txn, codeInvalidArgument)
	n.callEnded(watch, 0)
	set, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var sent atomic.Uint64
	h, err := newPromHandler(set, false, n.scrape, &sent)
	if err != nil {
		t.Fatal(err)
	}
	h.col.snap = n.scrape()
	for _, want := range []string{
		`grpc_server_handled_total{grpc_code="OK",grpc_method="Put",grpc_service="etcdserverpb.KV",grpc_type="unary"} 2`,
		`grpc_server_handled_total{grpc_code="InvalidArgument",grpc_method="Txn",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_handled_total{grpc_code="OK",grpc_method="Watch",grpc_service="etcdserverpb.Watch",grpc_type="bidi_stream"} 1`,
		`grpc_server_started_total{grpc_method="Status",grpc_service="etcdserverpb.Maintenance",grpc_type="unary"} 0`,
		`etcd_server_client_requests_total{client_api_version="unknown",type="unary"} 3`,
	} {
		name, _, _ := strings.Cut(want, "{")
		got, err := testutil.CollectAndFormat(h.col, expfmt.TypeTextPlain, name)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(got), want) {
			t.Errorf("the metrics lack %q:\n%s", want, got)
		}
	}
}
