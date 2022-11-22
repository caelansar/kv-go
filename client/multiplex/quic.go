package multiplex

import (
	"context"
	"crypto/tls"
	"io"

	"github.com/lucas-clemente/quic-go"
)

type QuicSession struct {
	quic.Connection
}

func (q *QuicSession) Open() (io.ReadWriteCloser, error) {
	return q.OpenStreamSync(context.Background())
}

func NewQuicSession(addr string, tlsConfig *tls.Config) (session Session, err error) {
	tlsConfig.NextProtos = []string{"h3"}
	conn, err := quic.DialAddr(addr, tlsConfig, nil)
	if err != nil {
		return
	}
	return &QuicSession{Connection: conn}, nil
}
