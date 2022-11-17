package common

func ArrayIntersection[T comparable](arrays ...[]T) []T {
	m := map[T]uint32{}
	for i := 0; i < len(arrays); i++ {
		for _, v := range arrays[i] {
			m[v] += 1
		}
	}
	rs := []T{}
	for k, v := range m {
		if v == uint32(len(arrays)) {
			rs = append(rs, k)
		}
	}
	return rs
}

func ArrayAggregate[T comparable](arrays ...[]T) []T {
	m := map[T]bool{}
	for i := 0; i < len(arrays); i++ {
		for _, v := range arrays[i] {
			m[v] = true
		}
	}
	rs := []T{}
	for k := range m {
		rs = append(rs, k)
	}
	return rs
}

func DeleteSinceWithIndex[T any](ss []T, idx int) []T {
	return append(ss[:idx], ss[idx+1:]...)
}
