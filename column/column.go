package column

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
)

var ErrNotFound = errors.New("collie: not found")

type Column interface {
	Add([]byte) error
	Get(int64) ([]byte, error)
	Len() int64
	Truncate(int64) error
	Close() error
}

// Abstract column methods

type abstract struct {
	file *os.File
	rows int64
}

func (c *abstract) Close() (err error) {
	if c.file != nil {
		err = c.file.Close()
		c.file = nil
	}
	return
}

func (c *abstract) Len() int64  { return atomic.LoadInt64(&c.rows) }
func (c *abstract) inc(n int64) { atomic.AddInt64(&c.rows, n) }
func (c *abstract) set(n int64) { atomic.StoreInt64(&c.rows, n) }
func (c *abstract) truncate(pos, off int64) error {
	err := c.file.Truncate(pos)
	if err == nil {
		c.set(off)
	}
	return err
}

// HELPERS

func openFile(fname string) (*os.File, int64, error) {
	file, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0664)
	if err != nil {
		return nil, 0, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, err
	}

	return file, info.Size(), nil
}

func checkNotFound(err error) error {
	if isNotFound(err) {
		return ErrNotFound
	}
	return err
}

func isNotFound(err error) bool {
	if pe, ok := err.(*os.PathError); ok && pe.Err == syscall.EINVAL {
		return true
	} else if err == io.EOF {
		return true
	}
	return false
}
