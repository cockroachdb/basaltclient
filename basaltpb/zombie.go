package basaltpb

// Zombie returns true if the object is a zombie (scheduled for deletion).
// A zombie object has no remaining namespace references.
func (r *StatResponse) Zombie() bool {
	return len(r.References) == 0
}
