package behavior_test

import (
	"log"
	"math/rand/v2"
	"testing"
)

func TestChacha8Rand(t *testing.T) {
	rnd := rand.NewChaCha8([32]byte{})
	x := rnd.Uint64()
	log.Println(x)
	if x != 12432809566553409497 {
		t.Error("bad")
	}
}
