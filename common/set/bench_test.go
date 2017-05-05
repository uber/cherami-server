package set

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

func BenchmarkSetInsert(b *testing.B) {

	for n := 1; n < 1048576; n *= 4 {

		var keys []string
		for i := 0; i < n; i++ {
			keys = append(keys, uuid.New().String())
		}

		for _, set := range []string{"SliceSet", "SortedSet", "MapSet"} {

			var s Set

			switch set {
			case "SliceSet":
				s = newSliceSet(n)
			case "SortedSet":
				s = newSortedSet(n)
			case "MapSet":
				s = newMapSet(n)
			}

			b.Run(fmt.Sprintf("%s/%d", set, n), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					s.Insert(keys[rand.Intn(n)])
				}
			})
		}
	}
}

func BenchmarkSetContains(b *testing.B) {

	for n := 1; n < 65536; n *= 2 {

		var keys []string
		for i := 0; i < n; i++ {
			keys = append(keys, uuid.New().String())
		}

		for _, set := range []string{"SliceSet", "SortedSet", "MapSet"} {

			var s Set

			switch set {
			case "SliceSet":
				s = newSliceSet(n)
			case "SortedSet":
				s = newSortedSet(n)
			case "MapSet":
				s = newMapSet(n)
			}

			for i := 0; i < n/2; i++ {
				s.Insert(keys[rand.Intn(n)])
			}

			b.Run(fmt.Sprintf("%s/%d", set, n), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					s.Contains(keys[rand.Intn(n)])
				}
			})
		}
	}
}
