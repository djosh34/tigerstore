package core

import (
	"errors"
	"math"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var errBadMessage = errors.New("core: malformed protobuf message")

// UnmarshalAlias decodes b into m like proto.Unmarshal, merging into what
// m holds, except that bytes fields are slices of b instead of copies. b
// must never change afterwards. The index keeps such slices, so a
// transaction's values stay in its Raft entry, the one copy of them.
func UnmarshalAlias(b []byte, m proto.Message) error {
	return decodeAlias(b, m.ProtoReflect())
}

func decodeAlias(b []byte, m protoreflect.Message) error {
	fields := m.Descriptor().Fields()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num > protowire.MaxValidNumber {
			return errBadMessage
		}
		tag := b[:n]
		b = b[n:]
		fd := fields.ByNumber(num)
		if fd == nil || !wireTypeFits(fd, typ) {
			// Unknown, or known with the wrong wire type: protobuf keeps
			// both as unknown fields.
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return errBadMessage
			}
			m.SetUnknown(append(append(m.GetUnknown(), tag...), b[:n]...))
			b = b[n:]
			continue
		}
		n, err := decodeField(b, typ, m, fd)
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
func decodeField(b []byte, typ protowire.Type, m protoreflect.Message, fd protoreflect.FieldDescriptor) (int, error) {
	switch {
	case fd.Kind() == protoreflect.MessageKind:
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
		return n, decodeAlias(v, sub)
	case fd.IsMap() || fd.Kind() == protoreflect.GroupKind:
		// None of etcd's requests have them.
		return 0, errBadMessage
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
				return protoreflect.Value{}, 0, errBadMessage
			}
			return protoreflect.ValueOfString(string(x)), n, nil
		}
		return protoreflect.ValueOfBytes(x[:len(x):len(x)]), n, nil
	default:
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
	}
	return protoreflect.Value{}, 0, errBadMessage
}
