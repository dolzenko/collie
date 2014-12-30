package collie

import (
	"errors"
	"regexp"
)

var validColumnName = regexp.MustCompile(`^[a-zA-Z]\w*$`)

// Column is an abstract column definition of a schema
type Column struct {
	// A column name, names must start with a letter,
	// followed by alphanumeric characters and underscores
	Name string
	// The maximum column length in bytes, assumed to be variable if <1
	Size int
	// Create an index for this column. Default: false
	Index bool
	// Do not store the data of this column, useful for
	// index-only columns
	NoData bool
}

func (c *Column) Validate() error {
	if c.Name == "" || !validColumnName.MatchString(c.Name) {
		return errors.New("collie: invalid column name '" + c.Name + "'")
	}
	return nil
}
