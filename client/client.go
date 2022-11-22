package client

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"io/ioutil"

	kvgo "github.com/caelansar/kv-go"
	"github.com/caelansar/kv-go/client/multiplex"
	abi "github.com/caelansar/kv-go/pb"
	"go.uber.org/zap"
)

type Client struct {
	logger  *zap.SugaredLogger
	codec   kvgo.Codec
	session multiplex.Session
}

type StreamResult struct {
	id uint32
	ch <-chan *abi.CommandResponse
}

func (s *StreamResult) Id() uint32 {
	return s.id
}

func (s *StreamResult) Chan() <-chan *abi.CommandResponse {
	return s.ch
}

func (c *Client) ExecuteStreaming(req *abi.CommandRequest) (*StreamResult, error) {
	stream, err := c.session.Open()
	if err != nil {
		return nil, err
	}
	protoReq, err := c.codec.Encode(req)
	if err != nil {
		return nil, err
	}
	c.logger.Debugf("write streaming req: %#v", req)
	_, err = stream.Write(protoReq)
	if err != nil {
		return nil, err
	}

	var ch = make(chan *abi.CommandResponse, 10)
	resp, err := c.codec.Decode(stream)
	if err != nil {
		c.logger.Errorw("failed to decode id", "err", err)
		return nil, err
	}
	id := resp.Values[0].GetInteger()
	c.logger.Debugw("get id success", "id", id)

	go func() {
		for {
			resp, err := c.codec.Decode(stream)
			if err != nil {
				c.logger.Errorw("failed to decode", "err", err, "eof", err == io.EOF)
				close(ch)
				break
			}
			if resp.Status == 0 {
				c.logger.Info("receive cancel")
				close(ch)
				break
			}
			ch <- resp
			c.logger.Debugw("get streaming resp", "resp", resp)
		}
	}()
	return &StreamResult{
		id: uint32(id),
		ch: ch,
	}, nil
}

func (c *Client) Execute(req *abi.CommandRequest) (*abi.CommandResponse, error) {
	stream, err := c.session.Open()
	if err != nil {
		return nil, err
	}
	protoReq, err := c.codec.Encode(req)
	if err != nil {
		return nil, err
	}
	c.logger.Debugf("write req: %#v", req)
	_, err = stream.Write(protoReq)
	if err != nil {
		return nil, err
	}
	resp, err := c.codec.Decode(stream)
	if err != nil {
		return nil, err
	}
	c.logger.Debugw("get resp", "resp", resp)
	return resp, nil
}

func NewTlsConfig() (*tls.Config, error) {
	key, err := parseKey("../certs/client.key")
	if err != nil {
		return nil, err
	}
	clientCert, err := parseCertificate("../certs/client.crt")
	if err != nil {
		return nil, err
	}
	caCert, err := parseCertificate("../certs/ca.crt")
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(caCert)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{clientCert.Raw},
				PrivateKey:  key,
			},
		},
		RootCAs:    certPool,
		ServerName: "kv.test.com",
	}
	return tlsConfig, nil
}

func NewClient(logger *zap.SugaredLogger, codec kvgo.Codec, session multiplex.Session) (client *Client, err error) {
	client = &Client{
		logger:  logger,
		codec:   codec,
		session: session,
	}
	return
}

func parseCertificate(crt string) (*x509.Certificate, error) {
	certPEMBlock, err := ioutil.ReadFile(crt)
	if err != nil {
		return nil, err
	}
	certDERBlock, _ := pem.Decode(certPEMBlock)
	return x509.ParseCertificate(certDERBlock.Bytes)
}

func parseKey(key string) (*rsa.PrivateKey, error) {
	keyPEMBlock, err := ioutil.ReadFile(key)
	if err != nil {
		return nil, err
	}

	keyDERBlock, _ := pem.Decode(keyPEMBlock)
	if keyDERBlock == nil {
		return nil, err
	}
	if keyDERBlock.Type == "RSA PRIVATE KEY" {
		key, err := x509.ParsePKCS1PrivateKey(keyDERBlock.Bytes)
		return key, err
	} else {
		return nil, errors.New("not support")
	}
}
