package kvgo

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"

	abi "github.com/caelansar/kv-go/pb"
	"github.com/golang/protobuf/proto"
)

const LENGTH = 4
const COMPRESSION_LIMIT = 1436
const COMPRESSION_BIT uint32 = 1 << 31

type Codec interface {
	Encode(*abi.CommandRequest) ([]byte, error)
	Decode(io.Reader) (*abi.CommandResponse, error)
}

type DefaultCodec struct {
}

func (d *DefaultCodec) Encode(req *abi.CommandRequest) ([]byte, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	length := len(data)

	if length > COMPRESSION_LIMIT {
		var compressedBytes bytes.Buffer
		w := gzip.NewWriter(&compressedBytes)
		_, err = w.Write(data)
		if err != nil {
			return nil, err
		}
		if err = w.Flush(); err != nil {
			return nil, err
		}
		if err = w.Close(); err != nil {
			return nil, err
		}
		err = binary.Write(&b, binary.BigEndian, uint32(len(compressedBytes.Bytes()))|COMPRESSION_BIT)
		if err != nil {
			return nil, err
		}
		b.Write(compressedBytes.Bytes())
	} else {
		err = binary.Write(&b, binary.BigEndian, uint32(length))
		if err != nil {
			return nil, err
		}
		b.Write(data)
	}
	return b.Bytes(), nil
}

func (d *DefaultCodec) Decode(reader io.Reader) (resp *abi.CommandResponse, err error) {
	var header uint32
	// read 4 bytes header first
	err = binary.Read(reader, binary.BigEndian, &header)
	if err != nil {
		return nil, err
	}

	var length = header & (^COMPRESSION_BIT)
	var compressed = header&COMPRESSION_BIT == COMPRESSION_BIT

	var b []byte
	if compressed {
		b = make([]byte, length)
		// read exactly `length` byte
		_, err = io.ReadFull(reader, b)
		if err != nil {
			return nil, err
		}
		r := bytes.NewReader(b)
		// new gzip reader with compressed data
		reader, err = gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		// uncompressed
		b, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
	} else {
		b = make([]byte, length)
		_, err = io.ReadFull(reader, b)
		if err != nil {
			return nil, err
		}
	}
	resp = &abi.CommandResponse{}
	err = proto.Unmarshal(b, resp)
	if err != nil {
		return nil, err
	}
	return resp, err
}
