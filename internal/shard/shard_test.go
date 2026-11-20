package shard

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestCheck(t *testing.T) {
	data := []byte("hello")
	block := binary.LittleEndian.AppendUint32(bytes.Clone(data), CRC(data))
	if got, ok := Check(block); !ok || !bytes.Equal(got, data) {
		t.Errorf("good block: %q, %v", got, ok)
	}
	block[0] ^= 1
	if _, ok := Check(block); ok {
		t.Error("flipped bit passed the CRC")
	}
	if _, ok := Check([]byte{1, 2, 3}); ok {
		t.Error("3-byte block passed")
	}
	if got, ok := Check(binary.LittleEndian.AppendUint32(nil, CRC(nil))); !ok || len(got) != 0 {
		t.Error("empty block (size 0 file) should pass")
	}
}

// Any intact block passes, a flipped bit in the data or the CRC fails, nothing panics.
func FuzzCheck(f *testing.F) {
	f.Add([]byte("hello"), uint32(0), uint8(0))
	f.Add([]byte{}, uint32(0), uint8(7))
	f.Add([]byte{1, 2, 3}, uint32(5), uint8(3))
	f.Fuzz(func(t *testing.T, b []byte, at uint32, bit uint8) {
		Check(b)
		block := binary.LittleEndian.AppendUint32(bytes.Clone(b), CRC(b))
		if got, ok := Check(block); !ok || !bytes.Equal(got, b) {
			t.Fatalf("intact block failed: %x", b)
		}
		i := int(at % uint32(len(block))) // one flip per input, not one per byte
		block[i] ^= 1 << (bit % 8)
		if _, ok := Check(block); ok {
			t.Fatalf("bit %d of byte %d flipped and passed: %x", bit%8, i, b)
		}
	})
}

func TestWait(t *testing.T) {
	c := NewClient("")
	if got := c.Wait(2*time.Second, "10.0.0.7:8081"); got != 2*time.Second {
		t.Errorf("no RTT known: %v", got)
	}
	c.RTT = func(string) time.Duration { return 150 * time.Millisecond } // far DC
	if got := c.Wait(2*time.Second, "10.0.0.7:8081"); got != 2600*time.Millisecond {
		t.Errorf("150ms away: %v, want 2.6s", got)
	}
}

func TestListen(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			c.Write([]byte("ok"))
			c.Close()
		}
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, _ := io.ReadAll(c)
	if string(b) != "ok" {
		t.Errorf("read %q", b)
	}
}
