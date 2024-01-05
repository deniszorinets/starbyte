package utils

func Insert[T any](arr []T, index int, value T) []T {
	if len(arr) == index {
		return append(arr, value)
	}
	arr = append(arr[:index+1], arr[index:]...)
	arr[index] = value
	return arr
}

func IndexOf[T comparable](arr []T, item T) int {
	for idx, i := range arr {
		if i == item {
			return idx
		}
	}
	return -1
}

func In[T comparable](arr []T, item T) bool {
	return IndexOf(arr, item) >= 0
}

func Intersect[T comparable](a []T, b []T) []T {
	result := []T{}
	for _, i := range a {
		if In(b, i) {
			result = append(result, i)
		}
	}
	return result
}
