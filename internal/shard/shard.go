package shard

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"time"

	"filescale/internal/db"
)

const Magic = "FSHD\x01"

// Stall is how long a stream may go without moving a block before it's dropped.
const Stall = 30 * time.Second

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func CRC(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Check splits a wire block (data + 4-byte little-endian CRC32C) and reports whether it's intact.
func Check(block []byte) ([]byte, bool) {
	n := len(block) - 4
	if n < 0 {
		return nil, false
	}
	return block[:n], CRC(block[:n]) == binary.LittleEndian.Uint32(block[n:])
}

func URL(addr string, store, file db.UUID, shard int) string {
	return fmt.Sprintf("http://%s/v1/%s/%s/%d", addr, store, file, shard)
}

// Client talks to the store API.
type Client struct {
	HTTP  *http.Client
	Token string
	RTT   func(addr string) time.Duration // round trip to a store, nil = LAN
}

func NewClient(token string) *Client {
	c := &Client{Token: token}
	var d net.Dialer
	c.HTTP = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, c.Wait(2*time.Second, addr))
			defer cancel()
			return d.DialContext(ctx, network, addr)
		},
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     60 * time.Second, // below the store's IdleTimeout
		// the serve's own 100 Continue timer decides, Go never sends a body unasked
		ExpectContinueTimeout: Stall,
	}}
	return c
}

// Wait is a LAN wait stretched for a far store: base + 4 round trips.
func (c *Client) Wait(base time.Duration, addr string) time.Duration {
	if c.RTT == nil {
		return base
	}
	return base + 4*c.RTT(addr)
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	return c.HTTP.Do(req)
}
