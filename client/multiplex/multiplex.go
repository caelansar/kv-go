package multiplex

import "io"

type Session interface {
	Open() (io.ReadWriteCloser, error)
}
