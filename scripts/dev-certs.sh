#!/usr/bin/env bash
# Makes a throwaway CA and certificates for a local 3-node cluster in the
# directory given as the first argument (default ./certs): ca.pem,
# node1..node3 (server and client use, for 127.0.0.1 and localhost),
# client, and proxy, which has no CommonName, as etcd's gRPC proxy wants
# of the certificate it dials with. CERT_IPS adds IP addresses to every
# certificate, space-separated. Not for production.
set -euo pipefail
dir=${1:-certs}
mkdir -p "$dir"
cd "$dir"
openssl ecparam -name prime256v1 -genkey -noout -out ca-key.pem
openssl req -x509 -new -key ca-key.pem -days 30 -subj "/CN=tigerstore dev CA" -out ca.pem
for name in node1 node2 node3 client proxy; do
	subj="/CN=$name"
	if [ "$name" = proxy ]; then
		subj="/O=tigerstore proxy"
	fi
	openssl ecparam -name prime256v1 -genkey -noout -out "$name-key.pem"
	openssl req -new -key "$name-key.pem" -subj "$subj" -out "$name.csr"
	san=IP:127.0.0.1,DNS:localhost
	for ip in ${CERT_IPS:-}; do san+=",IP:$ip"; done
	printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$san" > "$name.ext"
	openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
		-days 30 -extfile "$name.ext" -out "$name.pem" 2>/dev/null
	rm "$name.csr" "$name.ext"
done
echo "certificates in $PWD"
