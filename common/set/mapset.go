package set

type mapset struct {
	m map[string]struct{}
}

func newMapSet(cap int) *mapset {

	return &mapset{
		m: make(map[string]struct{}),
	}
}

func (t *mapset) Clear() {
	t.m = make(map[string]struct{})
}

func (t *mapset) Empty() bool {
	return len(t.m) == 0
}

func (t *mapset) Count() int {
	return len(t.m)
}

func (t *mapset) Insert(key string) {
	t.m[key] = struct{}{}
}

func (t *mapset) Contains(key string) bool {
	_, ok := t.m[key]
	return ok
}

func (t *mapset) Remove(key string) {
	delete(t.m, key)
}

func (t *mapset) Keys() map[string]struct{} {
	return t.m
}

func (t *mapset) Equals(s Set) bool {

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

func (t *mapset) Subset(s Set) bool {

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

func (t *mapset) Superset(s Set) bool {

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
