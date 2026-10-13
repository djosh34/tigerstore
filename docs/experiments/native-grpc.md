# Native gRPC frontend prototype

This worktree implements throwaway variant b for #74 experiment 1, based on main `eb8a20eda7b1436e3319b924753469f2d8c0ee99`. It replaces the TLS `grpc.Server.ServeHTTP` path with `grpc.Server.Serve`. The later variant c commit adds the #47 transport options. There is no environment selector. Build the commit for the variant you want. Neither commit changes core, node, WAL, peer transport, acknowledgement, durability or operation order.

This is a protocol adapter for an experiment, not code to copy into main without review. Local runs are targeted correctness tests only. The parent runs measurements and etcdctl smoke on ARM Actions.

## Upstream correction

Source inspected at tag v3.7.2:

- [server/embed/serve.go](https://github.com/etcd-io/etcd/blob/v3.7.2/server/embed/serve.go), `serve` and `grpcHandlerFunc`.
- [server/etcdserver/api/v3rpc/grpc.go](https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/grpc.go), gRPC credentials and options.
- [cmux v0.1.5 matchers.go](https://github.com/soheilhy/cmux/blob/v0.1.5/matchers.go), HTTP/2 content-type matching and initial SETTINGS.

etcd v3.7.2 uses cmux HTTP1/HTTP2 preface routing on its insecure shared port. Its secure shared port still uses `grpcHandlerFunc` and `grpc.Server.ServeHTTP`. Native secure gRPC is available when HTTP uses a separate port. This prototype uses cmux-like routing after TLS on one port. It does not reproduce etcd's secure shared-port implementation.

## Routing and TLS

The listener wraps each accepted socket in `tls.Server`. cmux reads decrypted bytes, including the TLS handshake, with a ten-second read deadline. It routes HTTP/2 connections whose first request has an `application/grpc` content-type prefix to native gRPC. Everything else goes to net/http. ALPN advertises both `h2` and `http/1.1`; ALPN alone does not choose the gRPC branch. Both HTTP/1 and HTTP/2 health/version requests remain supported.

cmux sends empty SETTINGS to unblock clients that wait for server SETTINGS before sending their first HEADERS. Go's HTTP/2 server rejects the extra SETTINGS ACK. The HTTP branch consumes exactly the counted matcher SETTINGS ACKs, without changing other frames. It passes bytes straight through after those ACKs. A small frame reader handles this startup case only; it is not a replacement HTTP/2 implementation. Native gRPC receives cmux's buffered connection directly. The ACK adapter does not read or copy bytes on the gRPC path.

A credentials adapter returns `credentials.TLSInfo` from the completed TLS handshake without doing TLS twice. It keeps peer certificates, verified chains, negotiated protocol and `PrivacyAndIntegrity` security level available through gRPC's peer context. Certificate verification still runs in crypto/tls with the configured CA and ClientAuth policy. The HTTP wrapper exposes `ConnectionState`, which Go 1.27 net/http recognizes, so HTTP ALPN and `Request.TLS` work despite cmux's connection wrapper.

Stop closes the listener and tracks all accepted sockets, including unfinished TLS handshakes and connections waiting for their first request. It closes active RPCs and HTTP connections abruptly, not gracefully. Serve waits for the two servers and cmux to finish and reports unexpected errors.

## Limitations

- cmux selects a whole connection from its first request, not each stream. Mixed HTTP and gRPC streams on one HTTP/2 connection are unsupported. A connection selected for gRPC cannot later serve `/version`; a connection selected for HTTP cannot later serve a gRPC RPC. Normal grpc-go and curl clients use separate connections.
- The SETTINGS adapter depends on cmux v0.1.5 sending empty SETTINGS and on clients acknowledging SETTINGS in order. It counts successful empty-frame writes, skips only zero-length stream-zero SETTINGS ACK frames, and preserves subsequent server ACKs. This needs more interoperability coverage before production use.
- cmux buffers the first request's headers and decodes HPACK during connection selection. It has no new explicit header-byte cap here. The read deadline bounds time, not memory. Slow or malformed clients do not have the same early rejection behavior as grpc-go's native listener. No security hardening is claimed.
- TLS configuration uses a cloned config with `h2` and `http/1.1`. Custom `GetConfigForClient` callbacks that override ALPN are not covered. No TLS exporter-based authentication or SPIFFE identity extraction is added. `TLSInfo.State` retains the original certificate URI SANs, but `TLSInfo.SPIFFEID` remains nil, unlike grpc-go's standard TLS credentials when they recognize a SPIFFE URI.
- The Go 1.27 wrapped-connection TLS behavior is a dependency. This HTTP wrapper is not portable to older net/http implementations without changes.
- There is no graceful shutdown, REST gateway, fallback ServeHTTP path, or runtime variant switch. HTTP endpoint bodies and health semantics are unchanged.

## Targeted tests

`TestNativeTLSRoutingAuthAndShutdown` checks HTTP/1 and HTTP/2 `/version`, repeated requests on the same connection, HTTP Request.TLS, rejection without a client certificate, native unary and streaming RPCs, gRPC TLS peer information, and shutdown with idle TLS and raw TCP clients. `TestNativeSettingsAckReader` checks exact ACK consumption and preservation of other frames. `TestNativeStopBeforeServe` checks the early-stop case.

Existing server tests cover health with and without a quorum, status, RangeStream fallback, TLS Put/Range/Txn, watch ordering, rejection of a certificate from another CA, node restart, and more than 250 streams on one connection. These are small correctness runs, not measurements.
