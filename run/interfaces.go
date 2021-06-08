package run

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

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
	stdErrBuffer := &bytes.Buffer{}
	doltSqlServer := exec.Command("dolt", "sql-server", fmt.Sprintf("-P=%d", c.port))
	doltSqlServer.Stderr = stdErrBuffer

	var serverErr error
	go func() {
		err := doltSqlServer.Run()
		if err != nil {
			if stdErrBuffer.Len() > 0 {
				serverErr = errors.New(stdErrBuffer.String())
				return
			} else {
				serverErr = err
				return
			}
		}
		if stdErrBuffer.Len() > 0 {
			serverErr = errors.New(stdErrBuffer.String())
			return
		}
	}()
	defer func() {
		killErr := doltSqlServer.Process.Kill()
		if err == nil && killErr != nil {
			err = errors.Wrap(killErr)
		}
	}()

	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", "root", "", "localhost", c.port), nil)
	if err != nil {
		return errors.Wrap(err)
	}
	_ = conn.Ping()
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

	return serverErr
}

// GetConnection returns a connection to the Dolt database for use with querying table data.
func (c *SqlServer) GetConnection() (*dbr.Connection, *os.Process, *bytes.Buffer, error) {
	stdErrBuffer := &bytes.Buffer{}
	doltSqlServer := exec.Command("dolt", "sql-server", fmt.Sprintf("-P=%d", c.port))
	doltSqlServer.Stderr = stdErrBuffer
	go func() {
		_ = doltSqlServer.Run()
	}()
	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", "root", "", "localhost", c.port), nil)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err)
	}
	_ = conn.Ping()
	_, err = conn.Exec(fmt.Sprintf("USE `%s`;", c.dbName))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err)
	}
	return conn, doltSqlServer.Process, stdErrBuffer, nil
}
