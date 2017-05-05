package set

import (
	"sort"
)

type sortedset struct {
	s []string
}

func newSortedSet(cap int) *sortedset {

	return &sortedset{
		s: make([]string, 0),
	}
}

func (t *sortedset) Clear() {
	t.s = make([]string, 0)
}

func (t *sortedset) Empty() bool {
	return len(t.s) == 0
}

func (t *sortedset) Count() int {
	return len(t.s)
}

func (t *sortedset) Insert(key string) {

	i := sort.SearchStrings(t.s, key)

	if i < len(t.s) && t.s[i] == key {
		return // already contains key
	}

	// make space and insert at index 'i'
	t.s = append(t.s, "")
	copy(t.s[i+1:], t.s[i:])
	t.s[i] = key
}

func (t *sortedset) Contains(key string) bool {

	i := sort.SearchStrings(t.s, key)
	return i < len(t.s) && t.s[i] == key
}

func (t *sortedset) Remove(key string) {

	i := sort.SearchStrings(t.s, key)

	if i == len(t.s) && t.s[i] != key {
		return // does not contain key
	}

	copy(t.s[i:], t.s[i+1:])
	t.s = t.s[:len(t.s)-1]
}

func (t *sortedset) Keys() map[string]struct{} {

	m := make(map[string]struct{}, len(t.s))

	for _, x := range t.s {
		m[x] = struct{}{}
	}

	return m
}

func (t *sortedset) Equals(s Set) bool {

	if s.Count() != t.Count() {
		return false
	}

	for k := range s.Keys() {
		if !t.Contains(k) {
			return false
		}
	}

	return true
}

func (t *sortedset) Subset(s Set) bool {

	if s.Count() < t.Count() {
		return false
	}

	for k := range t.Keys() {
		if !s.Contains(k) {
			return false
		}
	}

	return true
}

func (t *sortedset) Superset(s Set) bool {

	if s.Count() > t.Count() {
		return false
	}

	for k := range s.Keys() {
		if !t.Contains(k) {
			return false
		}
	}

	return true
}
