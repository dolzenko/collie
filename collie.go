package collie

import "errors"

var (
	ErrNotFound       = errors.New("collie: not found")
	ErrColumnNotFound = errors.New("collie: column not found")
)
