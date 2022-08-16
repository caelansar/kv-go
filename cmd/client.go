package main

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"io/ioutil"
	"strings"

	kvgo "github.com/caelansar/kv-go"
	abi "github.com/caelansar/kv-go/pb"
	"github.com/hashicorp/yamux"
	"go.uber.org/zap"
)

func main() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	client, err := NewClient("127.0.0.1:5000", l.Sugar(), &kvgo.DefaultCodec{})
	if err != nil {
		panic(err)
	}
	hset := &abi.CommandRequest_Hset{
		Hset: &abi.Hset{
			Table: "t1",
			Pair: &abi.Kvpair{
				Key: "k1",
				Value: &abi.Value{
					Value: &abi.Value_String_{strings.Repeat("f", kvgo.COMPRESSION_LIMIT+1)},
				},
			},
		},
	}
	_, err = client.execute(&abi.CommandRequest{RequestData: hset})
	if err != nil {
		panic(err)
	}

	hget := &abi.CommandRequest_Hget{
		Hget: &abi.Hget{
			Table: "t1",
			Key:   "k1",
		},
	}
	_, err = client.execute(&abi.CommandRequest{RequestData: hget})
	if err != nil {
		panic(err)
	}
}

type Client struct {
	logger *zap.SugaredLogger
	codec  kvgo.Codec
	stream io.ReadWriter
}

func (c *Client) execute(req *abi.CommandRequest) (*abi.CommandResponse, error) {
	protoReq, err := c.codec.Encode(req)
	if err != nil {
		return nil, err
	}
	c.logger.Debugf("write req: %#v", req)
	_, err = c.stream.Write(protoReq)
	if err != nil {
		return nil, err
	}
	resp, err := c.codec.Decode(c.stream)
	if err != nil {
		return nil, err
	}
	c.logger.Debugw("get resp", "resp", resp)
	return resp, nil
}

func NewClient(addr string, logger *zap.SugaredLogger, codec kvgo.Codec) (client *Client, err error) {
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
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{clientCert.Raw},
				PrivateKey:  key,
			},
		},
		RootCAs: certPool,
		NextProtos: []string{
			"kv",
		},
		ServerName: "kv.test.com",
	}

	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return
	}

	logger.Debug("new yamux client")
	session, err := yamux.Client(conn, nil)
	if err != nil {
		return
	}

	logger.Debug("yamux stream open")
	stream, err := session.Open()
	if err != nil {
		return
	}

	client = &Client{
		logger: logger,
		codec:  codec,
		stream: stream,
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
