package set

type sliceset struct {
	s []string
}

func newSliceSet(cap int) *sliceset {

	return &sliceset{
		s: make([]string, 0),
	}
}

func (t *sliceset) Clear() {
	t.s = make([]string, 0)
}

func (t *sliceset) Empty() bool {
	return len(t.s) == 0
}

func (t *sliceset) Count() int {
	return len(t.s)
}

func (t *sliceset) Insert(key string) {

	for _, x := range t.s {
		if x == key {
			return
		}
	}

	t.s = append(t.s, key)
}

func (t *sliceset) Contains(key string) bool {

	for _, x := range t.s {
		if x == key {
			return true
		}
	}

	return false
}

func (t *sliceset) Remove(key string) {

	for i, x := range t.s {

		if x == key {

			last := len(t.s) - 1

			if last >= 0 {
				t.s[i] = t.s[last]
			}

			t.s = t.s[:last]
			return
		}
	}

	return
}

func (t *sliceset) Keys() map[string]struct{} {

	m := make(map[string]struct{}, len(t.s))

	for _, x := range t.s {
		m[x] = struct{}{}
	}

	return m
}

func (t *sliceset) Equals(s Set) bool {

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

func (t *sliceset) Subset(s Set) bool {

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

func (t *sliceset) Superset(s Set) bool {

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
