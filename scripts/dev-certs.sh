#!/usr/bin/env bash
# Makes a throwaway CA and certificates for a local 3-node cluster in the
# directory given as the first argument (default ./certs): ca.pem,
# node1..node3 (server and client use, for 127.0.0.1 and localhost) and
# client. Not for production.
set -euo pipefail
dir=${1:-certs}
mkdir -p "$dir"
cd "$dir"
openssl ecparam -name prime256v1 -genkey -noout -out ca-key.pem
openssl req -x509 -new -key ca-key.pem -days 30 -subj "/CN=tigerstore dev CA" -out ca.pem
for name in node1 node2 node3 client; do
	openssl ecparam -name prime256v1 -genkey -noout -out "$name-key.pem"
	openssl req -new -key "$name-key.pem" -subj "/CN=$name" -out "$name.csr"
	printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n' > "$name.ext"
	openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
		-days 30 -extfile "$name.ext" -out "$name.pem" 2>/dev/null
	rm "$name.csr" "$name.ext"
done
echo "certificates in $PWD"
