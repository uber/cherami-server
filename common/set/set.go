package set

type Set interface {
	// Clear clears the set
	Clear()
	// Empty checks if the set is empty
	Empty() bool
	// Count returns the size of the set
	Count() int
	// Insert adds a key to the set
	Insert(key string)
	// Contains checks if the key exists in the set
	Contains(key string) bool
	// Remove removes the key from the set
	Remove(key string)
	// Keys returns the set of keys
	Keys() map[string]struct{}
	// Equals compares the set against another set
	Equals(s Set) bool
	// Subset checks if this set is a subset of another
	Subset(s Set) bool
	// Superset checks if this set is a superset of another
	Superset(s Set) bool
}

const sliceSetMax = 16

func NewSet(cap int) Set {

	if cap == 0 || cap > sliceSetMax {
		return newMapSet(cap)
	}

	return newSliceSet(cap)
}
