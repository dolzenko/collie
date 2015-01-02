package column

// A fixed-length column type
type Fixed struct {
	abstract
	maxLen int
}

// OpenFixed opens a file, containing fixed-length values of `maxLen` each
func OpenFixed(fname string, maxLen int) (*Fixed, error) {
	file, total, err := openFile(fname)
	if err != nil {
		return nil, err
	}
	return &Fixed{abstract{file, total / int64(maxLen)}, maxLen}, nil
}

func (c *Fixed) Get(offset int64) ([]byte, error) {
	min := offset * int64(c.maxLen)
	buf := make([]byte, c.maxLen)
	if _, err := c.file.ReadAt(buf, min); err != nil {
		return nil, checkNotFound(err)
	}
	return buf, nil
}

func (c *Fixed) Add(b []byte) error {
	if len(b) > int(c.maxLen) {
		b = b[:c.maxLen]
	}

	buf := make([]byte, c.maxLen)
	copy(buf, b)

	_, err := c.file.WriteAt(buf, c.Len()*int64(c.maxLen))
	if err == nil {
		c.inc(1)
	}
	return err
}

func (c *Fixed) Truncate(offset int64) error {
	return c.truncate(offset*int64(c.maxLen), offset)
}
