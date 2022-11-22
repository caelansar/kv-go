package main

import (
	"strings"
	"time"

	kvgo "github.com/caelansar/kv-go"
	"github.com/caelansar/kv-go/client"
	"github.com/caelansar/kv-go/client/multiplex"
	abi "github.com/caelansar/kv-go/pb"
	"go.uber.org/zap"
)

const addr = "127.0.0.1:5000"

func main() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	tlsConfig, err := client.NewTlsConfig()
	if err != nil {
		panic(err)
	}

	// session, err := multiplex.NewYamuxSession(addr, tlsConfig)
	// if err != nil {
	// 	panic(err)
	// }

	session, err := multiplex.NewQuicSession(addr, tlsConfig)
	if err != nil {
		panic(err)
	}

	client, err := client.NewClient(l.Sugar(), &kvgo.DefaultCodec{}, session)
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
	_, err = client.Execute(&abi.CommandRequest{RequestData: hset})
	if err != nil {
		panic(err)
	}

	hget := &abi.CommandRequest_Hget{
		Hget: &abi.Hget{
			Table: "t1",
			Key:   "k1",
		},
	}
	_, err = client.Execute(&abi.CommandRequest{RequestData: hget})
	if err != nil {
		panic(err)
	}

	publish := &abi.CommandRequest_Publish{
		Publish: &abi.Publish{
			Topic: "cae",
			Data: []*abi.Value{
				{
					Value: &abi.Value_String_{"hello"},
				},
				{
					Value: &abi.Value_String_{"world"},
				},
			},
		},
	}

	publish1 := &abi.CommandRequest_Publish{
		Publish: &abi.Publish{
			Topic: "cae",
			Data: []*abi.Value{
				{
					Value: &abi.Value_String_{"?"},
				},
			},
		},
	}

	subscribe := &abi.CommandRequest_Subscribe{
		Subscribe: &abi.Subscribe{
			Topic: "cae",
		},
	}

	sr, err := client.ExecuteStreaming(&abi.CommandRequest{RequestData: subscribe})
	if err != nil {
		panic(err)
	}

	go func() {
		_, err = client.Execute(&abi.CommandRequest{RequestData: publish})
		if err != nil {
			panic(err)
		}
		l.Sugar().Info("publish success")

		time.Sleep(100 * time.Millisecond)
		_, err = client.Execute(&abi.CommandRequest{RequestData: publish1})
		if err != nil {
			panic(err)
		}
		l.Sugar().Info("publish success")
	}()

	go func() {
		time.Sleep(500 * time.Millisecond)
		unsubscribe := &abi.CommandRequest_Unsubscribe{
			Unsubscribe: &abi.Unsubscribe{
				Topic: "cae",
				Id:    sr.Id(),
			},
		}
		_, err = client.Execute(&abi.CommandRequest{RequestData: unsubscribe})
		if err != nil {
			panic(err)
		}
		l.Sugar().Info("unsubscribe success")
	}()

	for data := range sr.Chan() {
		l.Sugar().Debugw("receive published data", "data", data, "subscription id", sr.Id())
	}
}
