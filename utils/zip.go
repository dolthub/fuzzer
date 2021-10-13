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

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/utils/file"
)

// ZipFile zips the file into an archive containing the given file as its root. May optionally delete the given file
// after it has been read.
func ZipFile(srcFile string, destFile string, deleteSrc bool) (err error) {
	dest, err := os.OpenFile(destFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	defer func() {
		fErr := dest.Close()
		if fErr != nil && err == nil {
			err = errors.Wrap(fErr)
		}
	}()

	zipWriter := zip.NewWriter(dest)
	defer func() {
		fErr := zipWriter.Close()
		if fErr != nil && err == nil {
			err = errors.Wrap(fErr)
		}
	}()

	err = zipFile(zipWriter, srcFile, filepath.Base(srcFile))
	if err != nil {
		return errors.Wrap(err)
	}
	if deleteSrc {
		return file.Remove(srcFile)
	}
	return nil
}

// ZipDirectory zips the contents of the entire directory, including all subdirectories. The top-level contents of the
// directory will comprise the root of the archive. May optionally delete the directory after it has been read.
func ZipDirectory(srcDir string, destFile string, deleteSrcDir bool) (err error) {
	srcDir = filepath.ToSlash(srcDir)

	dest, err := os.OpenFile(destFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	defer func() {
		fErr := dest.Close()
		if fErr != nil && err == nil {
			err = errors.Wrap(fErr)
		}
	}()

	zipWriter := zip.NewWriter(dest)
	defer func() {
		fErr := zipWriter.Close()
		if fErr != nil && err == nil {
			err = errors.Wrap(fErr)
		}
	}()

	err = filepath.Walk(srcDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Wrap(err)
		}
		if info.IsDir() {
			return nil
		}
		fileName := strings.TrimPrefix(strings.TrimPrefix(filepath.ToSlash(filePath), srcDir), "/")
		return zipFile(zipWriter, filePath, fileName)
	})
	if err != nil {
		return errors.Wrap(err)
	}
	if deleteSrcDir {
		return file.RemoveAll(srcDir)
	}
	return nil
}

// zipFile handles zipping each file into the given writer.
func zipFile(zipWriter *zip.Writer, filePath string, fileName string) (err error) {
	src, err := os.OpenFile(filePath, os.O_RDONLY, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	defer func() {
		fErr := src.Close()
		if fErr != nil && err == nil {
			err = errors.Wrap(fErr)
		}
	}()

	srcStat, err := src.Stat()
	if err != nil {
		return errors.Wrap(err)
	}
	srcLength := srcStat.Size()

	writer, err := zipWriter.Create(fileName)
	if err != nil {
		return errors.Wrap(err)
	}
	n, err := io.Copy(writer, src)
	if err != nil {
		return errors.Wrap(err)
	}
	if n != srcLength {
		return errors.New(fmt.Sprintf("Attempted to zip '%s', file is %d bytes but only wrote %d bytes", filePath, srcLength, n))
	}
	return nil
}
