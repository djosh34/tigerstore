module github.com/djosh34/tigerstore/audit

go 1.27.2

require (
	github.com/djosh34/tigerstore v0.0.0
	go.etcd.io/raft/v3 v3.7.0
)

require google.golang.org/protobuf v1.36.11 // indirect

replace github.com/djosh34/tigerstore => ../repo
