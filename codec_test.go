package kvgo

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
)

func TestGzip(t *testing.T) {
	var s = strings.Repeat("a", 2400)

	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	w.Flush()
	w.Close()

	r, err := gzip.NewReader(&b)
	if err != nil {
		t.Fatal(err)
	}
	res, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte(s), res) {
		t.Fatal("failed")
	}
}
