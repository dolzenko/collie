package collie

import "errors"

type Schema struct{ columns []Column }

// CreateSchema behaves like NewSchema, except that it panics on errors
func CreateSchema(cols []Column) *Schema {
	schema, err := NewSchema(cols)
	if err != nil {
		panic(err)
	}
	return schema
}

// NewSchema creates a new schema for a set of columns
func NewSchema(cols []Column) (*Schema, error) {
	schema := &Schema{columns: cols}
	known := make(map[string]bool, len(cols))

	for _, col := range schema.columns {
		if err := schema.validate(known, &col); err != nil {
			return nil, err
		}
	}
	return schema, nil
}

func (s *Schema) Columns() []Column { return s.columns }

func (s *Schema) validate(known map[string]bool, col *Column) (err error) {
	if err = col.Validate(); err != nil {
		return
	} else if _, ok := known[col.Name]; ok {
		return errors.New("collie: duplicate column '" + col.Name + "'")
	}
	known[col.Name] = true
	return
}
