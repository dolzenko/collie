package collie

import "errors"

var (
	ErrNotFound       = errors.New("collie: not found")
	ErrColumnNotFound = errors.New("collie: column not found")
)

type Encodable interface {
	EncodeAttr(string) ([]byte, error)
}
