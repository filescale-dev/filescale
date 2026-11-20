package shard

import (
	"context"
	"net"
	"syscall"
)

// Listen opens a TCP listener for serve and store traffic with segments capped at a
// 9000-byte jumbo frame, both ways (the peer sends at most the MSS we advertise). Over
// loopback (a serve and a store on one host) they're 64 KiB otherwise, and with
// net.ipv4.tcp_shrink_window=1 a receiver applying backpressure then drops so much
// in-flight data that resends back off for tens of seconds.
func Listen(addr string) (net.Listener, error) {
	lc := net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var err error
		if cerr := c.Control(func(fd uintptr) {
			err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, 8948)
		}); cerr != nil {
			return cerr
		}
		return err
	}}
	return lc.Listen(context.Background(), "tcp", addr)
}
