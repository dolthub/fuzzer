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

package rand

// This package is used for generating random numbers that is safe for concurrent use.

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/dolthub/fuzzer/errors"
	pkgErrors "github.com/pkg/errors"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
)

// This package is used for generating random numbers that is safe for concurrent use, specifically for the purposes of
// the fuzzer. String creation heavily relies on generating a byte array, and while both "math/rand" and "crypto/rand"
// offer a Read(int) function, calls are relatively expensive. We can amortize the cost by generating a large buffer
// upfront, and reading from the buffer. Additionally, benchmarking on a Windows PC showed a marginal increase in
// performance when using "crypto/rand" over "math/rand", which also gives better random results.

const (
	allowedChars       = ` !#$%*+-.0123456789:=@abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ^_|~`
	extAlphNumChars    = `0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ`
	allowedCharsLen    = byte(len(allowedChars))
	extAlphNumCharsLen = byte(len(extAlphNumChars))
)

var eofErr = pkgErrors.New("EOF")

var (
	// From testing on a single Windows PC, a buffer of 524288 was found to have the best overall performance.
	buffer      = make([]byte, 524288)
	idx         = 0
	mutex       = &sync.Mutex{}
	seedOutPath = ""
	seedInPath  = ""

	seedOutWriter *bufio.Writer
	seedInSize    int64
	seedInPos     int64
	seedInReader  *bufio.Reader
)

func Configure(seedInFilePath string, seedOutFilePath string) error {
	seedInPath = ""
	seedOutPath = ""
	seedInPos = -1
	seedInSize = -1
	seedInReader = nil
	seedOutWriter = nil
	var err error

	if seedInFilePath == seedOutFilePath {
		return errors.New("seed in and seed out files cannot be the same")
	}

	// configure input file
	if len(seedInFilePath) > 0 {
		seedInPath, err = filepath.Abs(seedInFilePath)
		if err != nil {
			fmt.Printf("error getting absolute path for seed in file %s: %s\n", seedInFilePath, err.Error())
			return err
		}
		fmt.Printf("seed in file path: %s\n", seedInPath)
		stat, err := os.Stat(seedInPath)
		if err != nil {
			fmt.Printf("error opening seed in file %s: %s\n", seedInPath, err.Error())
			return err
		}
		if stat.IsDir() {
			fmt.Printf("seed in file %s is a directory\n", seedInPath)
			return errors.New("seed in file is a directory")
		}

		file, err := os.Open(seedInPath)
		if err != nil {
			fmt.Printf("error opening seed in file %s: %s\n", seedInPath, err.Error())
			return err
		}

		seedInReader = bufio.NewReader(file)
		seedInSize = stat.Size()
		seedInPos = 0
	}

	// configure output file
	if len(seedOutFilePath) > 0 {
		seedOutPath, err = filepath.Abs(seedOutFilePath)
		if err != nil {
			fmt.Printf("error getting absolute path for seed out file %s: %s\n", seedOutFilePath, err.Error())
			return err
		}
		fmt.Printf("seed out file path: %s\n", seedOutPath)

		file, err := os.OpenFile(seedOutPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Printf("error opening seed out file %s: %s\n", seedOutPath, err.Error())
			return err
		}
		seedOutWriter = bufio.NewWriter(file)

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-signals

			seedInReader = nil
			if seedOutWriter == nil {
				return
			}

			err := seedOutWriter.Flush()
			if err != nil {
				fmt.Printf("error flushing seed out file: %s\n", err.Error())
				return
			}
			seedOutWriter = nil

			err = file.Close()
			if err != nil {
				fmt.Printf("error closing seed out file: %s\n", err.Error())
				return
			}
		}()
	}

	return nil
}
func readBytesFromSeedFile(length int) (readBytes int, data []byte, err error) {
	bytesRemaining := seedInSize - seedInPos
	if bytesRemaining == 0 {
		return 0, []byte{}, eofErr
	} else if bytesRemaining > int64(length) {
		buffer = make([]byte, length)
		read, err := seedInReader.Read(buffer)
		if err != nil {
			return -1, nil, err
		}
		seedInPos += int64(read)
		return read, buffer, nil
	} else {
		buffer = make([]byte, bytesRemaining)
		read, err := seedInReader.Read(buffer)
		if err != nil {
			return -1, nil, err
		}
		seedInPos += int64(read)
		return read, buffer, eofErr
	}
}
func writeBytesToSeedFile(data []byte) error {
	if seedOutWriter == nil {
		return nil
	}

	_, err := seedOutWriter.Write(data)
	if err != nil {
		fmt.Printf("error writing seed file: %s\n", err.Error())
		return err
	}

	return nil
}
func Finalize() (seedOutFilePath string, err error) {
	err = seedOutWriter.Flush()
	return seedOutPath, err
}

func init() {
	readBytes, err := rand.Read(buffer)
	if err != nil {
		panic(err)
	}
	if len(buffer) != readBytes {
		panic(fmt.Sprintf("expected %d but got %d", len(buffer), readBytes))
	}
}

// allocateAndReturnBytes returns a slice of bytes with the given length. Each byte slice returned has an independent
// underlying array, as the requested size may have been larger than the remaining bytes in the buffer.
func allocateAndReturnBytes(length int) ([]byte, error) {
	var err error
	data := make([]byte, length)
	mutex.Lock()
	for copiedBytes := 0; err == nil && copiedBytes < length; {
		n := copy(data[copiedBytes:], buffer[idx:])
		idx += n
		copiedBytes += n
		if idx >= len(buffer) {
			idx = 0
			var createdLength int
			// Because Bytes returns a slice with the underlying array, we don't want to overwrite any buffers out there
			buffer = make([]byte, len(buffer))
			createdLength, err = rand.Read(buffer)
			if err == nil {
				if createdLength != len(buffer) {
					err = errors.New(fmt.Sprintf("expected %d but got %d", len(buffer), createdLength))
				}
			}
		}
	}
	mutex.Unlock()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return data, nil
}

// Bytes returns a slice of bytes with the given length. The underlying array will usually be the buffer, therefore it
// is recommended to not use the string buffer shortcut for converting a byte array to string.
func Bytes(length int) ([]byte, error) {
	var data []byte
	var err error
	if seedInReader != nil {
		var readBytes int
		// try to get as many bytes from the file
		readBytes, data, err = readBytesFromSeedFile(length)
		if err != nil {
			// in case we run out of file, get the remaining bytes from crypto/rand
			if err == eofErr {
				length -= readBytes

				cryptoBytes, err := bytesFromCrypto(length)
				if err != nil {
					return nil, err
				}
				data = append(data, cryptoBytes...)
			} else {
				return nil, errors.New(fmt.Sprintf("error reading seed file %s: %s", seedInPath, err.Error()))
			}
		}
	} else {
		// no file reader setup, get all bytes from crypto/rand
		data, err = bytesFromCrypto(length)
		if err != nil {
			return nil, err
		}
	}

	err = writeBytesToSeedFile(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
func bytesFromCrypto(length int) ([]byte, error) {
	// On benchmarks from a single Windows PC, it was observed that lengths over 65536 begin to degrade in performance
	// versus "crypto/rand".Read().
	if length > 65536 {
		data := make([]byte, length)
		readBytes, err := rand.Read(data)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		if length != readBytes {
			return nil, errors.New(fmt.Sprintf("expected %d but got %d", length, readBytes))
		}
		return data, nil
	}
	// It is significantly quicker to return a slice than allocate an array, but this doesn't handle boundary slices.
	mutex.Lock()
	if idx+length < len(buffer) {
		data := buffer[idx : idx+length]
		idx += length
		mutex.Unlock()
		return data, nil
	}
	mutex.Unlock()
	data, err := allocateAndReturnBytes(length)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// String returns a random string. All characters will be ASCII between the inclusive decimal range of 32-126, with
// characters that are invalid in many contexts excluded, such as quotation characters.
func String(length int) (string, error) {
	v, err := Bytes(length)
	if err != nil {
		return "", errors.Wrap(err)
	}
	for i := 0; i < len(v); i++ {
		v[i] = allowedChars[v[i]%allowedCharsLen]
	}
	return string(v), nil
}

// StringCharSize returns the number of the available characters that may be used in a random string returned from String.
func StringCharSize() int64 {
	return int64(allowedCharsLen)
}

// StringExtendedAlphanumeric returns a random string. All characters will be alphanumeric, with the addition of the
// underscore.
func StringExtendedAlphanumeric(length int) (string, error) {
	v, err := Bytes(length)
	if err != nil {
		return "", errors.Wrap(err)
	}
	for i := 0; i < len(v); i++ {
		v[i] = extAlphNumChars[v[i]%extAlphNumCharsLen]
	}
	return string(v), nil
}

// StringExtendedAlphanumericCharSize returns the number of the available characters that may be used in a random string
// returned from StringExtendedAlphanumeric.
func StringExtendedAlphanumericCharSize() int64 {
	return int64(extAlphNumCharsLen)
}

// Int8 returns a random int8.
func Int8() (int8, error) {
	data, err := Bytes(1)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return int8(data[0]), nil
}

// Int16 returns a random int16.
func Int16() (int16, error) {
	data, err := Bytes(2)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return int16(binary.BigEndian.Uint16(data)), nil
}

// Int32 returns a random int32.
func Int32() (int32, error) {
	data, err := Bytes(4)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return int32(binary.BigEndian.Uint32(data)), nil
}

// Int64 returns a random int64.
func Int64() (int64, error) {
	data, err := Bytes(8)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

// Uint8 returns a random uint8.
func Uint8() (uint8, error) {
	data, err := Bytes(1)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return data[0], nil
}

// Uint16 returns a random uint16.
func Uint16() (uint16, error) {
	data, err := Bytes(2)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return binary.BigEndian.Uint16(data), nil
}

// Uint32 returns a random uint32.
func Uint32() (uint32, error) {
	data, err := Bytes(4)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return binary.BigEndian.Uint32(data), nil
}

// Uint64 returns a random uint64.
func Uint64() (uint64, error) {
	data, err := Bytes(8)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return binary.BigEndian.Uint64(data), nil
}

// Float32 returns a random float32 that is not Infinity nor NaN.
func Float32() (float32, error) {
	for {
		data, err := Bytes(4)
		if err != nil {
			return 0, errors.Wrap(err)
		}
		v := math.Float32frombits(binary.BigEndian.Uint32(data))
		if !f32IsInf(v) && !f32IsNaN(v) {
			return v, nil
		}
	}
}

// Float64 returns a random float64 that is not Infinity nor NaN.
func Float64() (float64, error) {
	for {
		data, err := Bytes(8)
		if err != nil {
			return 0, errors.Wrap(err)
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(data))
		if !math.IsInf(v, 0) && !math.IsNaN(v) {
			return v, nil
		}
	}
}

// Bool returns a random bool.
func Bool() (bool, error) {
	data, err := Bytes(1)
	if err != nil {
		return false, errors.Wrap(err)
	}
	return data[0] >= 128, nil
}

// f32IsInf is the float32 equivalent for math.IsInf
func f32IsInf(v float32) bool {
	return v > math.MaxFloat32 || v < -math.MaxFloat32
}

// f32IsNaN is the float32 equivalent for math.IsNaN
func f32IsNaN(v float32) bool {
	return v != v
}
