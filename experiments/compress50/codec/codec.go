package main

import (
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type codec struct {
	name       string
	level      int
	dictionary []byte
	ze         *zstd.Encoder
	zd         *zstd.Decoder
	lz         lz4.Compressor
	enc, dec   []byte
}

func newCodec(name string, level int, dictionary []byte) (*codec, error) {
	c := &codec{name: name, level: level, dictionary: dictionary}
	if name == "zstd" {
		eo := []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true), zstd.WithZeroFrames(true)}
		do := []zstd.DOption{zstd.WithDecoderConcurrency(1)}
		if len(dictionary) > 0 {
			eo = append(eo, zstd.WithEncoderDict(dictionary))
			do = append(do, zstd.WithDecoderDicts(dictionary))
		}
		var err error
		c.ze, err = zstd.NewWriter(nil, eo...)
		if err != nil {
			return nil, err
		}
		c.zd, err = zstd.NewReader(nil, do...)
		if err != nil {
			_ = c.ze.Close()
			return nil, err
		}
	}
	return c, nil
}
func (c *codec) close() error {
	if c.zd != nil {
		c.zd.Close()
	}
	if c.ze != nil {
		return c.ze.Close()
	}
	return nil
}
func resize(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}
func (c *codec) encode(src []byte) ([]byte, error) {
	switch c.name {
	case "zstd":
		c.enc = c.ze.EncodeAll(src, c.enc[:0])
	case "snappy":
		c.enc = snappy.Encode(c.enc[:cap(c.enc)], src)
	case "lz4":
		// LZ4 blocks alone cannot represent incompressible data or its raw length.
		// This wrapper stores u32 original length, then a 0=raw/1=compressed tag.
		if uint64(len(src)) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("LZ4 input exceeds u32 limit")
		}
		c.enc = resize(c.enc, 5+lz4.CompressBlockBound(len(src)))
		binary.LittleEndian.PutUint32(c.enc, uint32(len(src)))
		n, err := c.lz.CompressBlock(src, c.enc[5:])
		if err != nil {
			return nil, err
		}
		if n == 0 || n >= len(src) {
			c.enc[4] = 0
			copy(c.enc[5:], src)
			c.enc = c.enc[:5+len(src)]
		} else {
			c.enc[4] = 1
			c.enc = c.enc[:5+n]
		}
	default:
		return nil, fmt.Errorf("unknown codec")
	}
	return c.enc, nil
}
func (c *codec) decode(src []byte) ([]byte, error) {
	var err error
	switch c.name {
	case "zstd":
		c.dec, err = c.zd.DecodeAll(src, c.dec[:0])
	case "snappy":
		c.dec, err = snappy.Decode(c.dec[:cap(c.dec)], src)
	case "lz4":
		if len(src) < 5 {
			return nil, fmt.Errorf("short LZ4 wrapper")
		}
		n := int(binary.LittleEndian.Uint32(src))
		c.dec = resize(c.dec, n)
		switch src[4] {
		case 0:
			if len(src)-5 != n {
				return nil, fmt.Errorf("bad raw LZ4 length")
			}
			copy(c.dec, src[5:])
		case 1:
			var got int
			got, err = lz4.UncompressBlock(src[5:], c.dec)
			if err == nil && got != n {
				err = fmt.Errorf("bad LZ4 decoded length")
			}
		default:
			return nil, fmt.Errorf("bad LZ4 tag")
		}
	default:
		return nil, fmt.Errorf("unknown codec")
	}
	return c.dec, err
}
