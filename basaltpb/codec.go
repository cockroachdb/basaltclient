package basaltpb

import (
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/encoding"
	gproto "google.golang.org/protobuf/proto"
)

func init() {
	encoding.RegisterCodec(gogoCodec{})
}

// gogoCodec is a grpc codec that uses gogo/protobuf for marshaling when
// the type implements the gogo/protobuf interfaces, and falls back to
// google.golang.org/protobuf otherwise.
type gogoCodec struct{}

var _ encoding.Codec = gogoCodec{}

func (gogoCodec) Marshal(v any) ([]byte, error) {
	if pm, ok := v.(proto.Marshaler); ok {
		return pm.Marshal()
	}
	return gproto.Marshal(v.(gproto.Message))
}

func (gogoCodec) Unmarshal(data []byte, v any) error {
	if pm, ok := v.(proto.Unmarshaler); ok {
		return pm.Unmarshal(data)
	}
	return gproto.Unmarshal(data, v.(gproto.Message))
}

func (gogoCodec) Name() string {
	return "proto"
}
