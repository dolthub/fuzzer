package ranges

// String represents a range between two string values.
type String struct {
	Lowerbound string
	Upperbound string
}

// NewString converts a string slice into a String. Does not verify that the array has only two values.
func NewString(r []string) String {
	return String{
		Lowerbound: r[0],
		Upperbound: r[1],
	}
}

// NewStringCollection converts a slice of string slices into a []String. Does not verify that the collection is valid.
func NewStringCollection(r [][]string) []String {
	coll := make([]String, len(r))
	for i := range r {
		coll[i] = NewString(r[i])
	}
	return coll
}
