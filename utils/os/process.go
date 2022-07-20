// Copyright 2022 Dolthub, Inc.
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

//go:build !windows
// +build !windows

package os

import (
	"os"
	"os/exec"
)

// DisassociateExec allows for graceful closing of the generated process later on, depending on the operating system.
func DisassociateExec(cmd *exec.Cmd) {}

// CloseProcess attempts to gracefully close the process, without resorting to Kill(). Interrupt is not implemented on
// all operating systems, so this simulates the functionality on such platforms.
func CloseProcess(process *os.Process) error {
	return process.Signal(os.Interrupt)
}
