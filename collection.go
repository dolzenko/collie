package collie

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/bsm/collie/column"
)

type Collection struct {
	dir     string
	columns map[string]column.Column
	indices map[string]column.Index
	offset  int64
	wmux    sync.Mutex
}

// OpenCollection opens a collection in target directory for given schema
func OpenCollection(dir string, schema *Schema) (*Collection, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	coll := &Collection{
		dir:     dir,
		columns: make(map[string]column.Column),
		indices: make(map[string]column.Index),
	}

	// Register columns
	for _, col := range schema.Columns() {
		if err := coll.register(&col); err != nil {
			return nil, err
		}
	}

	// Re-establish offset (minimum)
	offset := int64(-1)
	for _, col := range coll.columns {
		if cln := col.Len(); offset < 0 || cln < offset {
			offset = cln
		}
	}
	if offset > 0 {
		coll.offset = offset
	}

	return coll, nil
}

// Begin starts a new transaction. The rows argument defines
// the capacity of the transactional cache and should hint
// at the number of rows about to be added.
func (c *Collection) Begin(rows int) *Txn { return newTxn(c, rows) }

// Offset returns the current offset
func (c *Collection) Offset() int64 { return atomic.LoadInt64(&c.offset) }

func (c *Collection) storeOffset(o int64) { atomic.StoreInt64(&c.offset, o) }

// Close closes the schema
func (c *Collection) Close() (err error) {
	for _, c := range c.columns {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	for _, x := range c.indices {
		if e := x.Close(); e != nil {
			err = e
		}
	}
	return
}

// Value returns a column value at a given offset
func (c *Collection) Value(name string, offset int64) ([]byte, error) {
	col, ok := c.columns[name]
	if !ok {
		return nil, ErrColumnNotFound
	}

	bin, err := col.Get(offset)
	if err == column.ErrNotFound {
		err = ErrNotFound
	}
	return bin, err
}

// Offsets returns a slice of offsets for a given index/value pair
func (c *Collection) Offsets(name string, value []byte) ([]int64, error) {
	idx, ok := c.indices[name]
	if !ok {
		return nil, ErrColumnNotFound
	}
	return idx.Get(value)
}

func (c *Collection) register(col *Column) (err error) {
	prefix := filepath.Join(c.dir, col.Name)

	switch col.Index {
	case IndexTypeHash:
		c.indices[col.Name], err = column.OpenHashIndex(prefix + ".ci")
	}

	if err == nil && !col.NoData {
		if col.Size > 0 {
			c.columns[col.Name], err = column.OpenFixed(prefix+".cc", col.Size)
		} else {
			c.columns[col.Name], err = column.OpenVariable(prefix + ".cc")
		}
	}
	return
}
