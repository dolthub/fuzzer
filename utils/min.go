package utils

import (
	"math"
)

// MinInt returns the minimum value between ints.
func MinInt(n ...int) int {
	m := maxInt
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinInt8 returns the minimum value between int8s.
func MinInt8(n ...int8) int8 {
	m := int8(math.MaxInt8)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinInt16 returns the minimum value between int16s.
func MinInt16(n ...int16) int16 {
	m := int16(math.MaxInt16)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinInt32 returns the minimum value between int32.
func MinInt32(n ...int32) int32 {
	m := int32(math.MaxInt32)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinInt64 returns the minimum value between int64s.
func MinInt64(n ...int64) int64 {
	m := int64(math.MaxInt64)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinUint returns the minimum value between uints.
func MinUint(n ...uint) uint {
	m := uint(maxUint)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinUint8 returns the minimum value between uint8s.
func MinUint8(n ...uint8) uint8 {
	m := uint8(math.MaxUint8)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinUint16 returns the minimum value between uint16s.
func MinUint16(n ...uint16) uint16 {
	m := uint16(math.MaxUint16)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinUint32 returns the minimum value between uint32s.
func MinUint32(n ...uint32) uint32 {
	m := uint32(math.MaxUint32)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}

// MinUint64 returns the minimum value between uint64s.
func MinUint64(n ...uint64) uint64 {
	m := uint64(math.MaxUint64)
	for i := 0; i < len(n); i++ {
		if n[i] < m {
			m = n[i]
		}
	}
	return m
}
