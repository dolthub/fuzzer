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

package run

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/gocraft/dbr/v2"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/ranges"
)

// Interface represents the SQL interfaces that dolt supports.
type Interface interface {
	ranges.Distributable
	// ProvideInterface handles starting and managing the interface, while providing the caller a function to pass
	// statements to.
	ProvideInterface(caller func(func(string) error) error) error
}

// CliQuery represents the CLI query interface, which is the --query argument.
type CliQuery struct {
	r      ranges.Int
	logger Logger
}

var _ Interface = (*CliQuery)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (c *CliQuery) GetOccurrenceRate() (int64, error) {
	return c.r.RandomValue()
}

// ProvideInterface implements the Interface interface.
func (c *CliQuery) ProvideInterface(caller func(func(string) error) error) error {
	stdErrBuffer := &bytes.Buffer{}
	return caller(func(statement string) error {
		err := c.logger.WriteLine(LogType_SQLQ, statement)
		if err != nil {
			return errors.Wrap(err)
		}
		doltQuery := exec.Command("dolt", "sql", "-q", statement)
		doltQuery.Env = env
		doltQuery.Stderr = stdErrBuffer
		err = doltQuery.Run()
		if err != nil {
			if stdErrBuffer.Len() > 0 {
				return errors.New(stdErrBuffer.String())
			} else {
				return errors.Wrap(err)
			}
		}
		if stdErrBuffer.Len() > 0 {
			return errors.New(stdErrBuffer.String())
		}
		return nil
	})
}

// CliBatch represents the CLI batch interface, which is when statements are piped in the sql command.
type CliBatch struct {
	r      ranges.Int
	logger Logger
}

var _ Interface = (*CliBatch)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (c *CliBatch) GetOccurrenceRate() (int64, error) {
	return c.r.RandomValue()
}

// ProvideInterface implements the Interface interface.
func (c *CliBatch) ProvideInterface(caller func(func(string) error) error) error {
	return caller(func(statement string) error {
		err := c.logger.WriteLine(LogType_SQLB, statement)
		if err != nil {
			return errors.Wrap(err)
		}
		stdErrBuffer := &bytes.Buffer{}
		doltSql := exec.Command("dolt", "sql")
		doltSql.Env = env
		doltSql.Stderr = stdErrBuffer
		doltSql.Stdin = bytes.NewBufferString(statement)
		err = doltSql.Run()
		if err != nil {
			if stdErrBuffer.Len() > 0 {
				return errors.New(stdErrBuffer.String())
			} else {
				return errors.Wrap(err)
			}
		}
		if stdErrBuffer.Len() > 0 {
			return errors.New(stdErrBuffer.String())
		}
		return nil
	})
}

// SqlServer represents the sql server interface.
type SqlServer struct {
	r      ranges.Int
	port   int64
	dbName string
	logger Logger
}

var _ Interface = (*SqlServer)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (c *SqlServer) GetOccurrenceRate() (int64, error) {
	return c.r.RandomValue()
}

// ProvideInterface implements the Interface interface.
func (c *SqlServer) ProvideInterface(caller func(func(string) error) error) (err error) {
	doltSqlServer := exec.Command("dolt", "sql-server", "-H=0.0.0.0", fmt.Sprintf("-P=%d", c.port))
	doltSqlServer.Env = env
	err = doltSqlServer.Start()
	if err != nil {
		return errors.Wrap(err)
	}
	defer func() {
		if doltSqlServer.Process != nil {
			killErr := doltSqlServer.Process.Kill()
			if err == nil && killErr != nil {
				err = errors.Wrap(killErr)
			}
		}
	}()

	// Wait for the process to start before continuing
	for exitLoop, timeout := false, time.After(5*time.Second); !exitLoop; {
		select {
		case <-timeout:
			return errors.New("unable to connect to dolt sql-server").Ignorable()
		default:
			if doltSqlServer.Process != nil {
				exitLoop = true
			}
		}
	}

	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", "root", "", "0.0.0.0", c.port), nil)
	if err != nil {
		return errors.Wrap(err)
	}

	// We continuously ping until we get a connection
	for timeout := time.After(5 * time.Second); conn.Ping() != nil; {
		select {
		case <-timeout:
			return errors.New("unable to connect to dolt sql-server").Ignorable()
		default:
		}
	}
	_, err = conn.Exec(fmt.Sprintf("USE `%s`;", c.dbName))
	if err != nil {
		return errors.Wrap(err)
	}
	err = caller(func(statement string) error {
		err = c.logger.WriteLine(LogType_SQLS, statement)
		if err != nil {
			return errors.Wrap(err)
		}
		_, err = conn.Exec(statement)
		if err != nil {
			return errors.Wrap(err)
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err)
	}
	err = conn.Close()
	if err != nil {
		return errors.Wrap(err)
	}

	return nil
}

// GetConnection returns a connection to the Dolt database for use with querying table data.
func (c *SqlServer) GetConnection() (*dbr.Connection, *os.Process, *bytes.Buffer, error) {
	stdErrBuffer := &bytes.Buffer{}
	doltSqlServer := exec.Command("dolt", "sql-server", "-H=0.0.0.0", fmt.Sprintf("-P=%d", c.port))
	doltSqlServer.Env = env
	doltSqlServer.Stderr = stdErrBuffer
	err := doltSqlServer.Start()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err)
	}

	// Wait for the process to start before continuing
	for exitLoop, timeout := false, time.After(5*time.Second); !exitLoop; {
		select {
		case <-timeout:
			return nil, nil, nil, errors.New("unable to connect to dolt sql-server").Ignorable()
		default:
			if doltSqlServer.Process != nil {
				exitLoop = true
			}
		}
	}

	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", "root", "", "0.0.0.0", c.port), nil)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err)
	}

	// We continuously ping until we get a connection
	for timeout := time.After(5 * time.Second); conn.Ping() != nil; {
		select {
		case <-timeout:
			return nil, nil, nil, errors.New("unable to connect to dolt sql-server").Ignorable()
		default:
		}
	}

	_, err = conn.Exec(fmt.Sprintf("USE `%s`;", c.dbName))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err)
	}
	return conn, doltSqlServer.Process, stdErrBuffer, nil
}
