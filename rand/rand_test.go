package rand_test

import (
	"bufio"
	"github.com/dolthub/fuzzer/rand"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestRand(t *testing.T) {
	inBytes := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}

	outFile, err := os.CreateTemp(".", "seed-out.bin")
	require.Nil(t, err)
	err = outFile.Close()
	require.Nil(t, err)
	defer func() {
		err = os.Remove(outFile.Name())
		require.Nil(t, err)
	}()

	inFile, err := os.CreateTemp(".", "seed-in.bin")
	require.Nil(t, err)
	defer func() {
		err = os.Remove(inFile.Name())
		require.Nil(t, err)
	}()
	inWriter := bufio.NewWriter(inFile)
	_, err = inWriter.Write(inBytes)
	require.Nil(t, err)
	err = inWriter.Flush()
	require.Nil(t, err)
	err = inFile.Close()
	require.Nil(t, err)

	err = rand.Configure(inFile.Name(), outFile.Name())
	require.Nil(t, err)

	r1, err := rand.Int64()
	require.Nil(t, err)
	require.Equal(t, int64(283686952306183), r1)

	r2, err := rand.Int64()
	require.Nil(t, err)
	require.Equal(t, int64(579005069656919567), r2)

	seedOutPath, err := rand.Finalize()
	require.Nil(t, err)
	absSeedOutPath, err := filepath.Abs(outFile.Name())
	require.Equal(t, absSeedOutPath, seedOutPath)

	outBytes, err := os.ReadFile(seedOutPath)
	require.Nil(t, err)
	require.Equal(t, inBytes, outBytes)
}
