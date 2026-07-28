package database

import "errors"

// Sentinel errors for the DynamoDB backend. Callers use errors.Is() to
// classify errors returned by SpecReader and ResourceCRUD methods.
var (
	ErrNotFound           = errors.New("not found")
	ErrPreconditionFailed = errors.New("precondition failed")
	ErrAlreadyExists      = errors.New("already exists")
)

func IsNotFoundError(err error) bool           { return errors.Is(err, ErrNotFound) }
func IsPreconditionFailedError(err error) bool { return errors.Is(err, ErrPreconditionFailed) }
func IsAlreadyExistsError(err error) bool      { return errors.Is(err, ErrAlreadyExists) }

func NewNotFoundError() error           { return ErrNotFound }
func NewPreconditionFailedError() error { return ErrPreconditionFailed }
func NewAlreadyExistsError() error      { return ErrAlreadyExists }
