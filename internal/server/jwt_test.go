package server

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// writeKeys writes key's private and public halves as PEM files.
func writeKeys(t *testing.T, name string, key crypto.Signer) (priv, pub string) {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	priv, pub = filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".pub")
	if err := os.WriteFile(priv, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pub, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return priv, pub
}

// TestAuthTokenOptions follows etcd v3.7.2's --auth-token: the token
// types, the JWT options, the key each sign method takes, verify-only
// public keys, and the claims a token carries.
func TestAuthTokenOptions(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherEC, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rsaPriv, rsaPub := writeKeys(t, "rsa", rsaKey)
	ecPriv, ecPub := writeKeys(t, "ec", ecKey)
	edPriv, edPub := writeKeys(t, "ed", edKey)
	_, otherPub := writeKeys(t, "other", otherEC)
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("a shared secret"), 0o600); err != nil {
		t.Fatal(err)
	}

	if j, err := ParseAuthToken("simple"); j != nil || err != nil {
		t.Fatalf("simple: %v %v", j, err)
	}
	for _, bad := range []string{
		"nope",
		"jwt",
		"jwt,sign-method=XY999",
		"jwt,sign-method=RS256",
		"jwt,sign-method=RS256,priv-key=" + rsaPriv + ",priv-key=" + rsaPriv,
		"jwt,sign-method=RS256,priv-key",
		"jwt,sign-method=RS256,priv-key=" + rsaPriv + ",ttl=soon",
		"jwt,sign-method=ES256,priv-key=" + ecPriv + ",pub-key=" + otherPub,
		"jwt,sign-method=ES256,priv-key=" + rsaPriv,
		"jwt,sign-method=HS256,pub-key=" + secret,
		"jwt,sign-method=RS256,priv-key=/does/not/exist",
	} {
		if j, err := ParseAuthToken(bad); err == nil {
			t.Errorf("%q: %v, want an error", bad, j)
		}
	}

	for _, c := range []struct {
		opts       string
		verifyOnly bool
	}{
		{"jwt,sign-method=RS256,priv-key=" + rsaPriv + ",pub-key=" + rsaPub, false},
		{"jwt,sign-method=PS384,priv-key=" + rsaPriv, false},
		{"jwt,sign-method=ES256,priv-key=" + ecPriv + ",pub-key=" + ecPub + ",ttl=1m", false},
		{"jwt,sign-method=EdDSA,priv-key=" + edPriv + ",pub-key=" + edPub, false},
		{"jwt,sign-method=HS512,priv-key=" + secret, false},
		{"jwt,sign-method=ES256,pub-key=" + ecPub, true},
		{"jwt,sign-method=RS256,pub-key=" + rsaPub, true},
	} {
		j, err := ParseAuthToken(c.opts)
		if err != nil {
			t.Fatalf("%q: %v", c.opts, err)
		}
		tok, err := j.sign("alice", 7)
		if c.verifyOnly {
			if !errors.Is(err, errVerifyOnly) {
				t.Fatalf("%q signs: %v", c.opts, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", c.opts, err)
		}
		if user, rev, ok := j.info(tok); !ok || user != "alice" || rev != 7 {
			t.Fatalf("%q: info %q %d %v", c.opts, user, rev, ok)
		}
		if _, _, ok := j.info(breakSignature(tok)); ok {
			t.Fatalf("%q takes a token with a broken signature", c.opts)
		}
	}

	// A public key alone checks the tokens of its private key.
	signer, err := ParseAuthToken("jwt,sign-method=ES256,priv-key=" + ecPriv)
	if err != nil {
		t.Fatal(err)
	}
	checker, err := ParseAuthToken("jwt,sign-method=ES256,pub-key=" + ecPub)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := signer.sign("bob", 3)
	if err != nil {
		t.Fatal(err)
	}
	if user, rev, ok := checker.info(tok); !ok || user != "bob" || rev != 3 {
		t.Fatalf("verify-only: %q %d %v", user, rev, ok)
	}
	// Another sign method's token does not pass.
	hs, err := ParseAuthToken("jwt,sign-method=HS256,priv-key=" + secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := hs.info(tok); ok {
		t.Fatal("an HS256 provider takes an ES256 token")
	}
	// An expired token does not pass.
	short, err := ParseAuthToken("jwt,sign-method=HS256,ttl=-1m,priv-key=" + secret)
	if err != nil {
		t.Fatal(err)
	}
	old, err := short.sign("bob", 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := hs.info(old); ok {
		t.Fatal("an expired token passes")
	}
	if strings.Count(old, ".") != 2 {
		t.Fatalf("not a JWT: %q", old)
	}

	// --auth-token "" hands out no tokens.
	nop, err := ParseAuthToken("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nop.sign("bob", 1); !errors.Is(err, rpctypes.ErrGRPCAuthFailed) {
		t.Fatalf("no tokens: %v", err)
	}
	if _, _, ok := nop.info(tok); ok {
		t.Fatal("no tokens takes one")
	}
}

// breakSignature changes a character well inside a token's signature, all
// of whose bits count, unlike those of its last character.
func breakSignature(tok string) string {
	b := []byte(tok)
	i := len(b) - 5
	if b[i] == 'A' {
		b[i] = 'B'
	} else {
		b[i] = 'A'
	}
	return string(b)
}
