package set

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type settype int

func (t settype) String() string {
	switch t {
	case tSliceset:
		return "sliceset"
	case tSortedset:
		return "sortedset"
	case tMapset:
		return "mapset"
	}
	return "unknown"
}

const (
	_ settype = iota
	tSliceset
	tSortedset
	tMapset
)

func TestSet(t *testing.T) {

	testCases := []struct {
		set0 settype
		set1 settype
	}{
		{tSliceset, tSliceset},
		{tSliceset, tSortedset},
		{tSliceset, tMapset},
		{tSortedset, tSliceset},
		{tSortedset, tSortedset},
		{tSortedset, tMapset},
		{tMapset, tSliceset},
		{tMapset, tSortedset},
		{tMapset, tMapset},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("{%v,%v}", tc.set0, tc.set1), func(t *testing.T) {

			var s0, s1 Set

			switch tc.set0 {
			case tSliceset:
				s0 = newSliceSet(0)
			case tSortedset:
				s0 = newSortedSet(0)
			case tMapset:
				s0 = newMapSet(0)
			}

			switch tc.set1 {
			case tSliceset:
				s1 = newSliceSet(0)
			case tSortedset:
				s1 = newSortedSet(0)
			case tMapset:
				s1 = newMapSet(0)
			}

			assert.False(t, s0.Contains("foo"))
			assert.Equal(t, 0, s0.Count())

			s0.Insert("foo")
			assert.Equal(t, 1, s0.Count())
			assert.True(t, s0.Contains("foo"))

			assert.True(t, s1.Subset(s0))
			assert.False(t, s0.Subset(s1))
			assert.False(t, s1.Superset(s0))
			assert.True(t, s0.Superset(s1))
			assert.False(t, s1.Equals(s0))
			assert.False(t, s0.Equals(s1))

			s1.Insert("foo")
			assert.True(t, s1.Subset(s0))
			assert.True(t, s0.Subset(s1))
			assert.True(t, s1.Superset(s0))
			assert.True(t, s0.Superset(s1))
			assert.True(t, s1.Equals(s0))
			assert.True(t, s0.Equals(s1))

			s1.Insert("bar")
			assert.Equal(t, 2, s1.Count())
			assert.False(t, s1.Subset(s0))
			assert.True(t, s0.Subset(s1))
			assert.True(t, s1.Superset(s0))
			assert.False(t, s0.Superset(s1))
			assert.False(t, s1.Equals(s0))
			assert.False(t, s0.Equals(s1))

			s0.Insert("bar")
			assert.True(t, s1.Subset(s0))
			assert.True(t, s0.Subset(s1))
			assert.True(t, s1.Superset(s0))
			assert.True(t, s0.Superset(s1))
			assert.True(t, s1.Equals(s0))
			assert.True(t, s0.Equals(s1))

			s0.Clear()
			assert.Equal(t, 0, s0.Count())
		})
	}
}
