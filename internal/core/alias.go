package core

import (
	"errors"
	"math"
	"slices"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The decoding errors are protobuf's, with the prefix etcd v3.7.2's
// build of protobuf gives them, so a client sees the same message.
var (
	errBadMessage = errors.New("proto: cannot parse invalid wire-format data")
	errBadUTF8    = errors.New("string field contains invalid UTF-8")
)

// UnmarshalAlias decodes b into m like proto.Unmarshal, merging into what
// m holds, except that bytes fields are slices of b instead of copies. b
// must never change afterwards. The index keeps such slices, so a
// transaction's values stay in its Raft entry, the one copy of them.
func UnmarshalAlias(b []byte, m proto.Message) error {
	// The top message counts as one level, as in protobuf.
	return decodeAlias(b, m.ProtoReflect(), protowire.DefaultRecursionLimit-1)
}

// errTooDeep is protobuf's error for messages nested past its limit.
var errTooDeep = errors.New("proto: exceeded maximum recursion depth")

// decodeAlias decodes into m; depth is how many more levels of nested
// messages may follow, protobuf's recursion limit.
func decodeAlias(b []byte, m protoreflect.Message, depth int) error {
	fields := m.Descriptor().Fields()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num > protowire.MaxValidNumber {
			return errBadMessage
		}
		b = b[n:]
		fd := fields.ByNumber(num)
		if fd == nil || !wireTypeFits(fd, typ) {
			// Unknown, or known with the wrong wire type: protobuf keeps
			// both as unknown fields.
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return errBadMessage
			}
			// protobuf keeps the tag in its minimal encoding.
			m.SetUnknown(append(protowire.AppendTag(m.GetUnknown(), num, typ), b[:n]...))
			b = b[n:]
			continue
		}
		n, err := decodeField(b, typ, m, fd, depth)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

// wireTypeFits reports whether a field of fd's kind can come with wire
// type typ; a packed repeated scalar comes as bytes.
func wireTypeFits(fd protoreflect.FieldDescriptor, typ protowire.Type) bool {
	want := kindWireType(fd.Kind())
	return typ == want || (fd.IsList() && typ == protowire.BytesType && want != protowire.BytesType)
}

func kindWireType(k protoreflect.Kind) protowire.Type {
	switch k {
	case protoreflect.BoolKind, protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind:
		return protowire.VarintType
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		return protowire.Fixed32Type
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		return protowire.Fixed64Type
	case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind:
		return protowire.BytesType
	case protoreflect.GroupKind:
		return protowire.StartGroupType
	}
	return -1
}

// decodeField decodes one field value and returns the bytes it took.
func decodeField(b []byte, typ protowire.Type, m protoreflect.Message, fd protoreflect.FieldDescriptor, depth int) (int, error) {
	switch {
	case fd.IsMap() || fd.Kind() == protoreflect.GroupKind:
		// None of etcd's requests have them.
		return 0, errBadMessage
	case fd.Kind() == protoreflect.MessageKind:
		if depth <= 0 {
			return 0, errTooDeep
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return 0, errBadMessage
		}
		var sub protoreflect.Message
		if fd.IsList() {
			sub = m.Mutable(fd).List().AppendMutable().Message()
		} else {
			// A message field that comes again merges into the first, and
			// so does a oneof member; another member replaces it.
			sub = m.Mutable(fd).Message()
		}
		return n, decodeAlias(v, sub, depth-1)
	case fd.IsList() && typ == protowire.BytesType && kindWireType(fd.Kind()) != protowire.BytesType:
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return 0, errBadMessage
		}
		list := m.Mutable(fd).List()
		for len(v) > 0 {
			x, k, err := scalar(v, kindWireType(fd.Kind()), fd)
			if err != nil {
				return 0, err
			}
			list.Append(x)
			v = v[k:]
		}
		return n, nil
	}
	x, n, err := scalar(b, typ, fd)
	if err != nil {
		return 0, err
	}
	if fd.IsList() {
		m.Mutable(fd).List().Append(x)
	} else {
		m.Set(fd, x)
	}
	return n, nil
}

// scalar decodes one scalar value of fd's kind.
func scalar(b []byte, typ protowire.Type, fd protoreflect.FieldDescriptor) (protoreflect.Value, int, error) {
	var v uint64
	var n int
	switch typ {
	case protowire.VarintType:
		v, n = protowire.ConsumeVarint(b)
	case protowire.Fixed32Type:
		var x uint32
		x, n = protowire.ConsumeFixed32(b)
		v = uint64(x)
	case protowire.Fixed64Type:
		v, n = protowire.ConsumeFixed64(b)
	case protowire.BytesType:
		var x []byte
		x, n = protowire.ConsumeBytes(b)
		if n < 0 {
			return protoreflect.Value{}, 0, errBadMessage
		}
		if fd.Kind() == protoreflect.StringKind {
			if !utf8.Valid(x) {
				return protoreflect.Value{}, 0, errBadUTF8
			}
			return protoreflect.ValueOfString(string(x)), n, nil
		}
		return protoreflect.ValueOfBytes(slices.Clip(x)), n, nil
	case protowire.StartGroupType, protowire.EndGroupType:
		return protoreflect.Value{}, 0, errBadMessage
	}
	if n < 0 {
		return protoreflect.Value{}, 0, errBadMessage
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(v != 0), n, nil
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(int32(v))), n, nil
	case protoreflect.Int32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(int32(v)), n, nil
	case protoreflect.Sint32Kind:
		return protoreflect.ValueOfInt32(int32(protowire.DecodeZigZag(v & 0xffffffff))), n, nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(uint32(v)), n, nil
	case protoreflect.Int64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(int64(v)), n, nil
	case protoreflect.Sint64Kind:
		return protoreflect.ValueOfInt64(protowire.DecodeZigZag(v)), n, nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(v), n, nil
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(math.Float32frombits(uint32(v))), n, nil
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(math.Float64frombits(v)), n, nil
	case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.GroupKind:
		// These never come as a number; wireTypeFits keeps them out.
	}
	return protoreflect.Value{}, 0, errBadMessage
}
