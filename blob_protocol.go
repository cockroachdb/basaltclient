package basaltclient

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
)

// Protocol constants.
const (
	// ProtocolMagic is the magic byte that starts every request and response.
	ProtocolMagic byte = 0xBA

	// ObjectIDSize is the size of an object identifier (UUID) in bytes.
	ObjectIDSize = 16

	// RequestHeaderSize is the total size of a request header in bytes.
	// Magic(1) + OpCode(1) + ObjectID(16) + Offset(8) + Length(8) = 34
	RequestHeaderSize = 34

	// ResponseHeaderSize is the total size of a response header in bytes.
	// Magic(1) + Status(1) + Length(8) = 10
	ResponseHeaderSize = 10
)

// OpCode represents an operation code in the wire protocol.
type OpCode byte

// Operation codes for the wire protocol.
const (
	OpAppend     OpCode = 0x01
	OpAppendSync OpCode = 0x02 // Append + Sync in one round-trip (empty data = sync only)
	OpRead       OpCode = 0x03
)

// String returns the string representation of an OpCode.
func (op OpCode) String() string {
	switch op {
	case OpAppend:
		return "Append"
	case OpAppendSync:
		return "AppendSync"
	case OpRead:
		return "Read"
	default:
		return "Unknown"
	}
}

// StatusCode represents a response status code.
type StatusCode byte

// Status codes for response messages.
const (
	StatusOK            StatusCode = 0x00
	StatusNotFound      StatusCode = 0x01
	StatusAlreadyExists StatusCode = 0x02
	StatusSealed        StatusCode = 0x03
	StatusIOError       StatusCode = 0x04
	StatusInvalidOp     StatusCode = 0x05
	StatusBadRequest    StatusCode = 0x06
)

// String returns the string representation of a StatusCode.
func (s StatusCode) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusNotFound:
		return "NotFound"
	case StatusAlreadyExists:
		return "AlreadyExists"
	case StatusSealed:
		return "Sealed"
	case StatusIOError:
		return "IOError"
	case StatusInvalidOp:
		return "InvalidOp"
	case StatusBadRequest:
		return "BadRequest"
	default:
		return "Unknown"
	}
}

// Error returns an error for non-OK status codes.
func (s StatusCode) Error() error {
	switch s {
	case StatusOK:
		return nil
	case StatusNotFound:
		return ErrNotFound
	case StatusAlreadyExists:
		return ErrAlreadyExists
	case StatusSealed:
		return ErrSealed
	case StatusIOError:
		return ErrIOError
	case StatusInvalidOp:
		return ErrInvalidOp
	case StatusBadRequest:
		return ErrBadRequest
	default:
		return errors.Newf("unknown status: %d", s)
	}
}

// Common errors returned by the protocol.
var (
	ErrNotFound      = errors.New("object not found")
	ErrAlreadyExists = errors.New("object already exists")
	ErrSealed        = errors.New("object is sealed")
	ErrIOError       = errors.New("I/O error")
	ErrInvalidOp     = errors.New("invalid operation")
	ErrBadRequest    = errors.New("bad request")
)

// ObjectID is a 16-byte unique identifier for an object.
type ObjectID [ObjectIDSize]byte

// RequestHeader represents a request message header.
type RequestHeader struct {
	OpCode   OpCode
	ObjectID ObjectID
	Offset   uint64
	Length   uint64
}

// Encode writes the request header to a byte slice.
// The slice must be at least RequestHeaderSize bytes.
func (h RequestHeader) Encode(buf []byte) {
	buf[0] = ProtocolMagic
	buf[1] = byte(h.OpCode)
	copy(buf[2:18], h.ObjectID[:])
	binary.BigEndian.PutUint64(buf[18:26], h.Offset)
	binary.BigEndian.PutUint64(buf[26:34], h.Length)
}

// DecodeRequestHeader reads a request header from a byte slice.
// The slice must be at least RequestHeaderSize bytes.
func DecodeRequestHeader(buf []byte) (RequestHeader, error) {
	if len(buf) < RequestHeaderSize {
		return RequestHeader{}, errors.Newf("buffer too small: %d < %d", len(buf), RequestHeaderSize)
	}
	if buf[0] != ProtocolMagic {
		return RequestHeader{}, errors.Newf("invalid magic: %x", buf[0])
	}
	var h RequestHeader
	h.OpCode = OpCode(buf[1])
	copy(h.ObjectID[:], buf[2:18])
	h.Offset = binary.BigEndian.Uint64(buf[18:26])
	h.Length = binary.BigEndian.Uint64(buf[26:34])
	return h, nil
}

// WriteRequest writes a complete request (header + optional data) to a writer.
func WriteRequest(w io.Writer, h RequestHeader, data []byte) error {
	var buf [RequestHeaderSize]byte
	h.Encode(buf[:])
	if _, err := w.Write(buf[:]); err != nil {
		return errors.Wrap(err, "writing request header")
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return errors.Wrap(err, "writing request data")
		}
	}
	return nil
}

// ReadRequestHeader reads a request header from a reader.
func ReadRequestHeader(r io.Reader) (RequestHeader, error) {
	var buf [RequestHeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return RequestHeader{}, errors.Wrap(err, "reading request header")
	}
	return DecodeRequestHeader(buf[:])
}

// ResponseHeader represents a response message header.
type ResponseHeader struct {
	Status StatusCode
	Length uint64
}

// Encode writes the response header to a byte slice.
// The slice must be at least ResponseHeaderSize bytes.
func (h ResponseHeader) Encode(buf []byte) {
	buf[0] = ProtocolMagic
	buf[1] = byte(h.Status)
	binary.BigEndian.PutUint64(buf[2:10], h.Length)
}

// DecodeResponseHeader reads a response header from a byte slice.
// The slice must be at least ResponseHeaderSize bytes.
func DecodeResponseHeader(buf []byte) (ResponseHeader, error) {
	if len(buf) < ResponseHeaderSize {
		return ResponseHeader{}, errors.Newf("buffer too small: %d < %d", len(buf), ResponseHeaderSize)
	}
	if buf[0] != ProtocolMagic {
		return ResponseHeader{}, errors.Newf("invalid magic: %x", buf[0])
	}
	var h ResponseHeader
	h.Status = StatusCode(buf[1])
	h.Length = binary.BigEndian.Uint64(buf[2:10])
	return h, nil
}

// WriteResponse writes a complete response (header + optional data) to a writer.
func WriteResponse(w io.Writer, status StatusCode, data []byte) error {
	var buf [ResponseHeaderSize]byte
	h := ResponseHeader{Status: status, Length: uint64(len(data))}
	h.Encode(buf[:])
	if _, err := w.Write(buf[:]); err != nil {
		return errors.Wrap(err, "writing response header")
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return errors.Wrap(err, "writing response data")
		}
	}
	return nil
}

// ReadResponseHeader reads a response header from a reader.
func ReadResponseHeader(r io.Reader) (ResponseHeader, error) {
	var buf [ResponseHeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return ResponseHeader{}, errors.Wrap(err, "reading response header")
	}
	return DecodeResponseHeader(buf[:])
}
