package column

import (
	"encoding/binary"
	"os"
	"sync"
)

// A variable-length column type
// Slower than fixed, but (potentially) more space-efficient
type Variable struct {
	abstract

	pos   int64
	bfile *os.File
	lock  sync.Mutex
}

// OpenFlexVariable opens a file, containing variable-length values
func OpenVariable(fname string) (*Variable, error) {
	file, size, err := openFile(fname + ".index")
	if err != nil {
		return nil, err
	}

	col := &Variable{abstract: abstract{file, size / 8}}
	if col.bfile, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0664); err != nil {
		col.Close()
		return nil, err
	} else if col.pos, err = col.offset(col.rows - 1); err != nil && err != ErrNotFound {
		col.Close()
		return nil, err
	}
	return col, nil
}

func (c *Variable) Close() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	err = c.abstract.Close()
	if c.bfile != nil {
		if e := c.bfile.Close(); e != nil {
			err = e
		}
		c.bfile = nil
	}
	return
}

func (c *Variable) Get(offset int64) ([]byte, error) {
	max, err := c.offset(offset)
	if err != nil {
		return nil, err
	}

	min := int64(0)
	if offset > 0 {
		if min, err = c.offset(offset - 1); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, max-min)
	if _, err = c.bfile.ReadAt(buf, min); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *Variable) Len() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.rows
}

func (c *Variable) Add(b []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	pos := c.pos + int64(len(b))
	row := c.rows + 1
	bps := make([]byte, 8)
	binary.BigEndian.PutUint64(bps, uint64(pos))

	_, err := c.bfile.WriteAt(b, c.pos)
	if err != nil {
		return err
	} else if _, err = c.file.WriteAt(bps, c.rows*8); err != nil {
		return err
	}

	c.rows = row
	c.pos = pos
	return nil
}

func (c *Variable) Truncate(rows int64) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var pos int64
	if rows > 0 {
		if pos, err = c.offset(rows - 1); err != nil {
			return
		}
	} else {
		rows = 0
	}

	if err = c.file.Truncate(rows * 8); err != nil {
		return
	}
	c.rows = rows
	c.pos = pos
	return nil
}

func (c *Variable) offset(i int64) (int64, error) {
	buf := make([]byte, 8)
	if _, err := c.file.ReadAt(buf, i*8); err != nil {
		return 0, checkNotFound(err)
	}
	return int64(binary.BigEndian.Uint64(buf)), nil
}
