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

package connection

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/gocraft/dbr/v2"

	"github.com/dolthub/fuzzer/errors"
)

// DoltConnection represents a running Dolt process along with its connection to the server. This allows for individual
// calls to the server to reuse the process and connection (as creating new processes is extremely slow). This
// connection and its associated process is automatically closed whenever any non-server calls are made (CLI calls being
// one such example).
type DoltConnection struct {
	Conn         *dbr.Connection
	Process      *os.Process
	StdErrBuffer *bytes.Buffer
	dbName       string
	port         int64
}

var (
	dcLock               sync.Mutex
	globalDoltConnection *DoltConnection
	env                  = os.Environ()
)

// GetDoltConnection returns an existing connection if one exists and matches the parameters. If an existing one does
// not match the parameters, then it is automatically closed. Otherwise, it creates a new one.
func GetDoltConnection(port int64, dbName string) (*DoltConnection, error) {
	dcLock.Lock()
	defer dcLock.Unlock()

	// Reuse the global Dolt connection if it already exists and has the same parameters that we desire
	if globalDoltConnection != nil {
		if globalDoltConnection.port == port && globalDoltConnection.dbName == dbName {
			return globalDoltConnection, nil
		}
		// If a global connection exists but has a different port or db name, then we close it to open a new one
		err := func() error {
			// Must unlock first so that there's no deadlock
			dcLock.Unlock()
			defer dcLock.Lock()

			gdc := globalDoltConnection
			globalDoltConnection = nil
			err := gdc.Close()
			if err != nil {
				return errors.Wrap(err)
			}
			return nil
		}()
		if err != nil {
			return nil, errors.Wrap(err)
		}
	}

	stdErrBuffer := &bytes.Buffer{}
	doltSqlServer := exec.Command("dolt", "sql-server", "-H=0.0.0.0", fmt.Sprintf("-P=%d", port))
	doltSqlServer.Env = env
	doltSqlServer.Stderr = stdErrBuffer
	err := doltSqlServer.Start()
	if err != nil {
		return nil, errors.Wrap(err)
	}

	// Wait for the process to start before continuing
	for exitLoop, timeout := false, time.After(5*time.Second); !exitLoop; {
		select {
		case <-timeout:
			return nil, errors.New("unable to connect to dolt sql-server").Ignorable()
		default:
			if doltSqlServer.Process != nil {
				exitLoop = true
			}
		}
	}

	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", "root", "", "0.0.0.0", port), nil)
	if err != nil {
		_ = doltSqlServer.Process.Kill()
		return nil, errors.Wrap(err)
	}

	// We continuously ping until we get a connection
	for timeout := time.After(5 * time.Second); conn.Ping() != nil; {
		select {
		case <-timeout:
			_ = conn.Close()
			_ = doltSqlServer.Process.Kill()
			return nil, errors.New("unable to connect to dolt sql-server").Ignorable()
		default:
		}
	}

	_, err = conn.Exec(fmt.Sprintf("USE `%s`;", dbName))
	if err != nil {
		_ = conn.Close()
		_ = doltSqlServer.Process.Kill()
		return nil, errors.Wrap(err)
	}
	globalDoltConnection = &DoltConnection{
		Conn:         conn,
		Process:      doltSqlServer.Process,
		StdErrBuffer: stdErrBuffer,
		dbName:       dbName,
		port:         port,
	}
	return globalDoltConnection, nil
}

// CloseDoltConnections closes all open Dolt connections. If there are no connection, then this is a no-op.
func CloseDoltConnections() error {
	return globalDoltConnection.Close()
}

// Close closes the DoltConnection, which is assumed to be the global. Therefore, this also sets the global connection
// to nil.
func (conn *DoltConnection) Close() error {
	dcLock.Lock()
	defer dcLock.Unlock()

	if conn == nil {
		return nil
	}
	cErr := conn.Conn.Close()
	pErr := conn.Process.Kill()
	// Check errors in reverse order
	if pErr != nil {
		return errors.Wrap(pErr)
	}
	if cErr != nil {
		return errors.Wrap(cErr)
	}
	globalDoltConnection = nil
	return nil
}
