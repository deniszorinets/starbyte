package utils

func HasKey[T comparable, V any](m map[T]V, key T) bool {
	_, ok := m[key]
	return ok
}

func MergeMap[T comparable, V any](a map[T]V, b map[T]V) map[T]V {
	for k, v := range b {
		a[k] = v
	}
	return a
}

func Keys[T comparable, V any](a map[T]V) []T {
	keys := []T{}
	for k := range a {
		keys = append(keys, k)
	}
	return keys
}
