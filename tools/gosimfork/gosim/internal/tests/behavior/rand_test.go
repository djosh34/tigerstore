//go:build sim

package behavior_test

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/maphash"
	"log/slog"
	mathrand "math/rand"
	mathrandv2 "math/rand/v2"
	"testing"
)

func TestRandMapHash(t *testing.T) {
	s := maphash.MakeSeed()
	slog.Info("rand", "val", fmt.Sprint(maphash.String(s, "foo")))
}

func TestRandMathFloat64(t *testing.T) {
	slog.Info("rand", "val", fmt.Sprint(mathrand.Float64()))
}

func TestRandMathV2Float64(t *testing.T) {
	slog.Info("rand", "val", fmt.Sprint(mathrandv2.Float64()))
}

func TestRandCrypto(t *testing.T) {
	var buf [32]byte
	if _, err := cryptorand.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	slog.Info("rand", "val", hex.EncodeToString(buf[:]))
}
