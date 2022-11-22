package multiplex

import (
	"crypto/tls"
	"io"

	"github.com/hashicorp/yamux"
)

type YamuxSession struct {
	*yamux.Session
}

func (y *YamuxSession) Open() (io.ReadWriteCloser, error) {
	return y.Session.Open()
}

func NewYamuxSession(addr string, tlsConfig *tls.Config) (session Session, err error) {
	tlsConfig.NextProtos = []string{"kv"}
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return
	}

	ys, err := yamux.Client(conn, nil)
	if err != nil {
		return
	}
	return &YamuxSession{Session: ys}, nil
}
