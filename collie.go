package collie

import "errors"

var (
	ErrNotFound       = errors.New("collie: not found")
	ErrColumnNotFound = errors.New("collie: column not found")
)

// Values are just byte arrays
type Value []byte

// A record is an abstract interface that returns
type Record interface {
	// ValueAt accepts a data column name and returns the
	// record attribute as a plain value
	ValueAt(string) (Value, error)
	// IValuesAt accepts an index column name and returns the
	// values indexing this record
	IValuesAt(string) ([]Value, error)
}

type Row struct {
	columns map[string]Value
	indices map[string][]Value
}

func newRow(ccap, icap int) *Row {
	return &Row{
		columns: make(map[string]Value, ccap),
		indices: make(map[string][]Value, icap),
	}
}

// AddIndex adds a value to an index
func (r *Row) AddIndex(index string, value Value) {
	r.indices[index] = append(r.indices[index], value)
}

// SetColumn sets the value of a column
func (r *Row) SetColumn(column string, value Value) { r.columns[column] = value }

func (r *Row) ValueAt(column string) (Value, error)    { return r.columns[column], nil }
func (r *Row) IValuesAt(index string) ([]Value, error) { return r.indices[index], nil }
