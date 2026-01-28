package blob

import (
	"bytes"
	"testing"
)

func TestRequestHeaderEncodeDecode(t *testing.T) {
	var id ObjectID
	copy(id[:], []byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78})
	original := RequestHeader{
		OpCode:   OpRead,
		ObjectID: id,
		Offset:   1000,
		Length:   4096,
	}

	var buf [RequestHeaderSize]byte
	original.Encode(buf[:])

	decoded, err := DecodeRequestHeader(buf[:])
	if err != nil {
		t.Fatalf("DecodeRequestHeader failed: %v", err)
	}

	if decoded.OpCode != original.OpCode {
		t.Errorf("OpCode: got %v, want %v", decoded.OpCode, original.OpCode)
	}
	if decoded.ObjectID != original.ObjectID {
		t.Errorf("ObjectID: got %v, want %v", decoded.ObjectID, original.ObjectID)
	}
	if decoded.Offset != original.Offset {
		t.Errorf("Offset: got %v, want %v", decoded.Offset, original.Offset)
	}
	if decoded.Length != original.Length {
		t.Errorf("Length: got %v, want %v", decoded.Length, original.Length)
	}
}

func TestResponseHeaderEncodeDecode(t *testing.T) {
	original := ResponseHeader{
		Status: StatusOK,
		Length: 12345,
	}

	var buf [ResponseHeaderSize]byte
	original.Encode(buf[:])

	decoded, err := DecodeResponseHeader(buf[:])
	if err != nil {
		t.Fatalf("DecodeResponseHeader failed: %v", err)
	}

	if decoded.Status != original.Status {
		t.Errorf("Status: got %v, want %v", decoded.Status, original.Status)
	}
	if decoded.Length != original.Length {
		t.Errorf("Length: got %v, want %v", decoded.Length, original.Length)
	}
}

func TestRequestHeaderBadMagic(t *testing.T) {
	var buf [RequestHeaderSize]byte
	buf[0] = 0xFF // Wrong magic

	_, err := DecodeRequestHeader(buf[:])
	if err == nil {
		t.Fatal("Expected error for bad magic")
	}
}

func TestResponseHeaderBadMagic(t *testing.T) {
	var buf [ResponseHeaderSize]byte
	buf[0] = 0xFF // Wrong magic

	_, err := DecodeResponseHeader(buf[:])
	if err == nil {
		t.Fatal("Expected error for bad magic")
	}
}

func TestWriteReadRequest(t *testing.T) {
	var id ObjectID
	copy(id[:], []byte{0xaa, 0xaa, 0xbb, 0xbb, 0xcc, 0xcc, 0xdd, 0xdd, 0xee, 0xee, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	header := RequestHeader{
		OpCode:   OpAppendSync,
		ObjectID: id,
		Offset:   500,
		Length:   100,
	}
	data := []byte("hello world")

	var buf bytes.Buffer
	if err := WriteRequest(&buf, header, data); err != nil {
		t.Fatalf("WriteRequest failed: %v", err)
	}

	decoded, err := ReadRequestHeader(&buf)
	if err != nil {
		t.Fatalf("ReadRequestHeader failed: %v", err)
	}

	if decoded.OpCode != header.OpCode {
		t.Errorf("OpCode: got %v, want %v", decoded.OpCode, header.OpCode)
	}
	if decoded.ObjectID != header.ObjectID {
		t.Errorf("ObjectID: got %v, want %v", decoded.ObjectID, header.ObjectID)
	}

	// Read remaining data
	remaining := buf.Bytes()
	if !bytes.Equal(remaining, data) {
		t.Errorf("Data: got %q, want %q", remaining, data)
	}
}

func TestWriteReadResponse(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteResponse(&buf, StatusNotFound, nil); err != nil {
		t.Fatalf("WriteResponse failed: %v", err)
	}

	decoded, err := ReadResponseHeader(&buf)
	if err != nil {
		t.Fatalf("ReadResponseHeader failed: %v", err)
	}

	if decoded.Status != StatusNotFound {
		t.Errorf("Status: got %v, want %v", decoded.Status, StatusNotFound)
	}
	if decoded.Length != 0 {
		t.Errorf("Length: got %v, want %v", decoded.Length, 0)
	}
}

func TestStatusCodeError(t *testing.T) {
	tests := []struct {
		status StatusCode
		want   error
	}{
		{StatusOK, nil},
		{StatusNotFound, ErrNotFound},
		{StatusAlreadyExists, ErrAlreadyExists},
		{StatusSealed, ErrSealed},
		{StatusIOError, ErrIOError},
		{StatusBadRequest, ErrBadRequest},
		{StatusInvalidOp, ErrInvalidOp},
	}

	for _, tt := range tests {
		got := tt.status.Error()
		if got != tt.want {
			t.Errorf("StatusCode(%v).Error(): got %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestAllOpCodes(t *testing.T) {
	ops := []OpCode{OpAppend, OpAppendSync, OpRead}
	for _, op := range ops {
		header := RequestHeader{
			OpCode:   op,
			ObjectID: ObjectID{},
			Offset:   0,
			Length:   0,
		}

		var buf [RequestHeaderSize]byte
		header.Encode(buf[:])

		decoded, err := DecodeRequestHeader(buf[:])
		if err != nil {
			t.Errorf("DecodeRequestHeader for op %v failed: %v", op, err)
			continue
		}
		if decoded.OpCode != op {
			t.Errorf("OpCode: got %v, want %v", decoded.OpCode, op)
		}
	}
}

func TestAllStatusCodes(t *testing.T) {
	statuses := []StatusCode{
		StatusOK, StatusNotFound, StatusAlreadyExists,
		StatusSealed, StatusIOError, StatusInvalidOp, StatusBadRequest,
	}
	for _, status := range statuses {
		header := ResponseHeader{
			Status: status,
			Length: 42,
		}

		var buf [ResponseHeaderSize]byte
		header.Encode(buf[:])

		decoded, err := DecodeResponseHeader(buf[:])
		if err != nil {
			t.Errorf("DecodeResponseHeader for status %v failed: %v", status, err)
			continue
		}
		if decoded.Status != status {
			t.Errorf("Status: got %v, want %v", decoded.Status, status)
		}
	}
}

func TestOpCodeString(t *testing.T) {
	tests := []struct {
		op   OpCode
		want string
	}{
		{OpAppend, "Append"},
		{OpAppendSync, "AppendSync"},
		{OpRead, "Read"},
		{OpCode(0xFF), "Unknown"},
	}

	for _, tt := range tests {
		got := tt.op.String()
		if got != tt.want {
			t.Errorf("OpCode(%d).String(): got %q, want %q", tt.op, got, tt.want)
		}
	}
}

func TestStatusCodeString(t *testing.T) {
	tests := []struct {
		status StatusCode
		want   string
	}{
		{StatusOK, "OK"},
		{StatusNotFound, "NotFound"},
		{StatusAlreadyExists, "AlreadyExists"},
		{StatusSealed, "Sealed"},
		{StatusIOError, "IOError"},
		{StatusInvalidOp, "InvalidOp"},
		{StatusBadRequest, "BadRequest"},
		{StatusCode(0xFF), "Unknown"},
	}

	for _, tt := range tests {
		got := tt.status.String()
		if got != tt.want {
			t.Errorf("StatusCode(%d).String(): got %q, want %q", tt.status, got, tt.want)
		}
	}
}
