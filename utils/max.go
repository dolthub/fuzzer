package utils

import (
	"math"
	"math/bits"
)

const (
	maxInt  = 1<<(bits.UintSize-1) - 1
	minInt  = -maxInt - 1
	maxUint = 1<<bits.UintSize - 1
)

// MaxInt returns the maximum value between ints.
func MaxInt(n ...int) int {
	m := minInt
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxInt8 returns the maximum value between int8s.
func MaxInt8(n ...int8) int8 {
	m := int8(math.MinInt8)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxInt16 returns the maximum value between int16s.
func MaxInt16(n ...int16) int16 {
	m := int16(math.MinInt16)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxInt32 returns the maximum value between int32s.
func MaxInt32(n ...int32) int32 {
	m := int32(math.MinInt32)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxInt64 returns the maximum value between int64s.
func MaxInt64(n ...int64) int64 {
	m := int64(math.MinInt64)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxUint returns the maximum value between uints.
func MaxUint(n ...uint) uint {
	m := uint(0)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxUint8 returns the maximum value between uint8s.
func MaxUint8(n ...uint8) uint8 {
	m := uint8(0)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxUint16 returns the maximum value between uint16s.
func MaxUint16(n ...uint16) uint16 {
	m := uint16(0)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxUint32 returns the maximum value between uint32s.
func MaxUint32(n ...uint32) uint32 {
	m := uint32(0)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}

// MaxUint64 returns the maximum value between uint64s.
func MaxUint64(n ...uint64) uint64 {
	m := uint64(0)
	for i := 0; i < len(n); i++ {
		if n[i] > m {
			m = n[i]
		}
	}
	return m
}
