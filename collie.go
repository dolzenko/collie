package collie

import "errors"

var (
	ErrNotFound       = errors.New("collie: not found")
	ErrColumnNotFound = errors.New("collie: column not found")
)

type (
	EncodeColumnFunc func(string) ([]byte, error)
	EncodeIndexFunc  func(string) ([][]byte, error)
)

type Encodable interface {
	EncodeColumn(string) ([]byte, error)
	EncodeIndex(string) ([][]byte, error)
}
