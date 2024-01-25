// Package slice is a utils for working with slice.
package slice

// ConvertFuncWithSkip ...
func ConvertFuncWithSkip[From, To any](slice []From, convertFunc func(elem From) (To, bool)) []To {
	result := make([]To, 0, len(slice))

	for _, elem := range slice {
		resElem, skip := convertFunc(elem)
		if skip {
			continue
		}

		result = append(result, resElem)
	}

	return result
}

// ConvertFunc ...
func ConvertFunc[From, To any](slice []From, convertFunc func(elem From) To) []To {
	return ConvertFuncWithSkip(
		slice,
		func(elem From) (To, bool) {
			return convertFunc(elem), false
		},
	)
}
