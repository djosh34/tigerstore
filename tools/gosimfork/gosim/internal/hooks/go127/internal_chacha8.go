package go127

import (
	_ "unsafe"
)

//go:linkname block gosimnotranslate/internal/chacha8rand.block
func block(seed *[4]uint64, blocks *[32]uint64, counter uint32)

func InternalChacha8rand_block(seed *[4]uint64, blocks *[32]uint64, counter uint32) {
	block(seed, blocks, counter)
}
