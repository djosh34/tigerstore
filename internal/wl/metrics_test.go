package wl

import (
	"strings"
	"testing"
)

func TestMetricsNameCallsAsGRPCDoes(t *testing.T) {
	var m nodeMetrics
	m.count(kPut, 0)
	m.count(kPut, 0)
	m.count(kTxn, codeInvalidArgument)
	m.count(kWatch, 0)
	m.hasLeader.Store(1)
	text := string(m.text())
	for _, want := range []string{
		"\netcd_server_has_leader 1\n",
		`grpc_server_handled_total{grpc_code="OK",grpc_method="Put",grpc_service="etcdserverpb.KV",grpc_type="unary"} 2`,
		`grpc_server_handled_total{grpc_code="InvalidArgument",grpc_method="Txn",grpc_service="etcdserverpb.KV",grpc_type="unary"} 1`,
		`grpc_server_handled_total{grpc_code="OK",grpc_method="Watch",grpc_service="etcdserverpb.Watch",grpc_type="bidi_stream"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the metrics lack %q:\n%s", want, text)
		}
	}
}
