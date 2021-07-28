// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

// AbsInt returns the absolute value of an int.
func AbsInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// AbsInt8 returns the absolute value of an int8.
func AbsInt8(n int8) int8 {
	if n < 0 {
		return -n
	}
	return n
}

// AbsInt16 returns the absolute value of an int16.
func AbsInt16(n int16) int16 {
	if n < 0 {
		return -n
	}
	return n
}

// AbsInt32 returns the absolute value of an int32.
func AbsInt32(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}

// AbsInt64 returns the absolute value of an int64.
func AbsInt64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
