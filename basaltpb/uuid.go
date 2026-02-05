package basaltpb

import "github.com/google/uuid"

// UUID is a 16-byte unique identifier used for ObjectID, DirectoryID, and MountID.
// It's wire-compatible with bytes but avoids allocations.
type UUID [16]byte

// Marshal implements the gogo/protobuf Marshaler interface.
func (u UUID) Marshal() ([]byte, error) {
	return u[:], nil
}

// MarshalTo implements the gogo/protobuf MarshalTo interface.
func (u UUID) MarshalTo(data []byte) (int, error) {
	return copy(data, u[:]), nil
}

// Unmarshal implements the gogo/protobuf Unmarshaler interface.
func (u *UUID) Unmarshal(data []byte) error {
	copy(u[:], data)
	return nil
}

// Size implements the gogo/protobuf Sizer interface.
func (u UUID) Size() int {
	return 16
}

// ToUUID converts to github.com/google/uuid.UUID.
func (u UUID) ToUUID() uuid.UUID {
	return uuid.UUID(u)
}

// String returns the UUID as a string.
func (u UUID) String() string {
	return u.ToUUID().String()
}

// UUIDFromBytes creates a UUID from a byte slice.
func UUIDFromBytes(b []byte) UUID {
	var u UUID
	copy(u[:], b)
	return u
}

// UUIDFromUUID creates a UUID from a google/uuid.UUID.
func UUIDFromUUID(u uuid.UUID) UUID {
	return UUID(u)
}

// NewUUID generates a new random UUID.
func NewUUID() UUID {
	return UUID(uuid.New())
}

// IsZero returns true if the UUID is the zero value (nil UUID).
func (u UUID) IsZero() bool {
	return u == UUID{}
}
