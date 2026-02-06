package basaltpb

// Sealed returns true if the object has been sealed (immutable).
func (m *ObjectMeta) Sealed() bool {
	return m.SealedAtNanos != 0
}

// Sealed returns true if the entry's object has been sealed (immutable).
func (m *DirectoryEntry) Sealed() bool {
	return m.SealedAtNanos != 0
}
