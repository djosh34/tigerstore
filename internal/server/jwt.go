package server

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// JWT tokens follow etcd v3.7.2's tokenJWT (server/auth/jwt.go): the
// server signs a token with the user and the auth revision when an
// Authenticate applies, and checks the signature and expiry of every
// token it gets. The node then takes the user and revision from the
// claims; a token of an older auth revision fails with
// ErrAuthOldRevision, as in etcd.

// The errors of etcd's auth token options.
var (
	ErrInvalidAuthOpts   = errors.New("auth: invalid auth options")
	errInvalidAuthMethod = errors.New("auth: invalid auth signature method")
	errMissingKey        = errors.New("auth: missing key data")
	errKeyMismatch       = errors.New("auth: public and private keys don't match")
	errVerifyOnly        = errors.New("auth: token signing attempted with verify-only key")
)

// JWT signs and checks JWT tokens. With --auth-token "", etcd's
// tokenNop, it hands out none and takes none.
type JWT struct {
	nop    bool
	method jwt.SigningMethod
	// key is the private key, or the public key alone, which only
	// checks tokens, or the HMAC secret.
	key        any
	ttl        time.Duration
	verifyOnly bool
}

// ParseAuthToken parses etcd's --auth-token: "simple", which gives a nil
// JWT, "jwt,pub-key=<file>,priv-key=<file>,sign-method=<alg>,ttl=<d>",
// or "", which gives no tokens at all.
func ParseAuthToken(opts string) (*JWT, error) {
	parts := strings.Split(opts, ",")
	m := map[string]string{}
	for _, kv := range parts[1:] {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || strings.Contains(v, "=") {
			return nil, ErrInvalidAuthOpts
		}
		if _, dup := m[k]; dup {
			return nil, ErrInvalidAuthOpts
		}
		m[k] = v
	}
	switch parts[0] {
	case "simple":
		return nil, nil
	case "jwt":
		return newJWT(m)
	case "":
		return &JWT{nop: true}, nil
	}
	return nil, ErrInvalidAuthOpts
}

func newJWT(m map[string]string) (*JWT, error) {
	t := &JWT{ttl: 5 * time.Minute}
	var err error
	if ttl := m["ttl"]; ttl != "" {
		if t.ttl, err = time.ParseDuration(ttl); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidAuthOpts, err)
		}
	}
	var pubPEM, privPEM []byte
	if f := m["pub-key"]; f != "" {
		if pubPEM, err = os.ReadFile(f); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidAuthOpts, err)
		}
	}
	if f := m["priv-key"]; f != "" {
		if privPEM, err = os.ReadFile(f); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidAuthOpts, err)
		}
	}
	if t.method = jwt.GetSigningMethod(m["sign-method"]); t.method == nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidAuthOpts, errInvalidAuthMethod)
	}
	switch t.method.(type) {
	case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
		t.key, t.verifyOnly, err = pairKey(privPEM, pubPEM, parseRSAPrivate, parseRSAPublic)
	case *jwt.SigningMethodECDSA:
		t.key, t.verifyOnly, err = pairKey(privPEM, pubPEM, parseECPrivate, parseECPublic)
	case *jwt.SigningMethodEd25519:
		t.key, t.verifyOnly, err = pairKey(privPEM, pubPEM, parseEdPrivate, parseEdPublic)
	case *jwt.SigningMethodHMAC:
		if len(privPEM) == 0 {
			return nil, errMissingKey
		}
		t.key = privPEM
	default:
		return nil, fmt.Errorf("unsupported signing method: %T", t.method)
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

// pairKey is etcd's choice of key: the private key, checked against the
// public one if both are given, or the public key alone, which only
// checks tokens.
func pairKey(privPEM, pubPEM []byte, parsePriv func([]byte) (crypto.Signer, error), parsePub func([]byte) (crypto.PublicKey, error)) (key any, verifyOnly bool, err error) {
	var priv crypto.Signer
	var pub crypto.PublicKey
	if len(privPEM) > 0 {
		if priv, err = parsePriv(privPEM); err != nil {
			return nil, false, err
		}
	}
	if len(pubPEM) > 0 {
		if pub, err = parsePub(pubPEM); err != nil {
			return nil, false, err
		}
	}
	if priv == nil {
		if pub == nil {
			return nil, false, errMissingKey
		}
		return pub, true, nil
	}
	if pub != nil {
		if p, ok := pub.(interface{ Equal(crypto.PublicKey) bool }); !ok || !p.Equal(priv.Public()) {
			return nil, false, errKeyMismatch
		}
	}
	return priv, false, nil
}

func parseRSAPrivate(b []byte) (crypto.Signer, error) {
	k, err := jwt.ParseRSAPrivateKeyFromPEM(b)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func parseRSAPublic(b []byte) (crypto.PublicKey, error) {
	k, err := jwt.ParseRSAPublicKeyFromPEM(b)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func parseECPrivate(b []byte) (crypto.Signer, error) {
	k, err := jwt.ParseECPrivateKeyFromPEM(b)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func parseECPublic(b []byte) (crypto.PublicKey, error) {
	k, err := jwt.ParseECPublicKeyFromPEM(b)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func parseEdPrivate(b []byte) (crypto.Signer, error) {
	k, err := jwt.ParseEdPrivateKeyFromPEM(b)
	if err != nil {
		return nil, err
	}
	priv, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, jwt.ErrNotEdPrivateKey
	}
	return priv, nil
}

func parseEdPublic(b []byte) (crypto.PublicKey, error) {
	return jwt.ParseEdPublicKeyFromPEM(b)
}

// sign is etcd's tokenJWT.assign.
func (t *JWT) sign(user string, revision uint64) (string, error) {
	if t.nop {
		return "", rpctypes.ErrGRPCAuthFailed
	}
	if t.verifyOnly {
		return "", errVerifyOnly
	}
	return jwt.NewWithClaims(t.method, jwt.MapClaims{
		"username": user,
		"revision": revision,
		"exp":      time.Now().Add(t.ttl).Unix(),
	}).SignedString(t.key)
}

// info is etcd's tokenJWT.info: the user and revision of a valid token.
func (t *JWT) info(token string) (user string, revision uint64, ok bool) {
	if t.nop {
		return "", 0, false
	}
	parsed, err := jwt.Parse(token, func(tk *jwt.Token) (any, error) {
		if tk.Method.Alg() != t.method.Alg() {
			return nil, errInvalidAuthMethod
		}
		switch k := t.key.(type) {
		case *rsa.PrivateKey:
			return &k.PublicKey, nil
		case *ecdsa.PrivateKey:
			return &k.PublicKey, nil
		case ed25519.PrivateKey:
			return k.Public(), nil
		}
		return t.key, nil
	})
	if err != nil || !parsed.Valid {
		return "", 0, false
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return "", 0, false
	}
	user, ok = claims["username"].(string)
	if !ok {
		return "", 0, false
	}
	rev, ok := claims["revision"].(float64)
	if !ok {
		return "", 0, false
	}
	return user, uint64(rev), true
}
