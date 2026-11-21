package cluster

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sockaddr "github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/memberlist"
)

const (
	udpPacketBufSize = 65536
	udpRecvBufSize   = 2 * 1024 * 1024
)

// reuseTransport binds memberlist TCP+UDP with SO_REUSEPORT for overlapping reloads.
type reuseTransport struct {
	bindAddr     string
	logger       *log.Logger
	tcpLn        *net.TCPListener
	udpConn      *net.UDPConn
	packetCh     chan *memberlist.Packet
	streamCh     chan net.Conn
	shutdownCh   chan struct{}
	shutdown     atomic.Bool
	shutdownOnce sync.Once
	wg           sync.WaitGroup
}

var _ memberlist.NodeAwareTransport = (*reuseTransport)(nil)

func newReuseTransport(bindAddr string, bindPort int, logger *log.Logger) (*reuseTransport, error) {
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	lc := net.ListenConfig{Control: reuseControl}
	limit := 1
	if bindPort == 0 {
		limit = 10
	}
	var (
		tcpLn   *net.TCPListener
		udpConn *net.UDPConn
		lastErr error
	)
	for range limit {
		port := bindPort
		ln, err := lc.Listen(context.Background(), "tcp", net.JoinHostPort(bindAddr, strconv.Itoa(port)))
		if err != nil {
			lastErr = fmt.Errorf("failed to start TCP listener on %q port %d: %w", bindAddr, port, err)
			if strings.Contains(err.Error(), "address already in use") {
				continue
			}
			return nil, lastErr
		}
		tl := ln.(*net.TCPListener)
		if port == 0 {
			port = tl.Addr().(*net.TCPAddr).Port
		}
		pc, err := lc.ListenPacket(context.Background(), "udp", net.JoinHostPort(bindAddr, strconv.Itoa(port)))
		if err != nil {
			_ = tl.Close()
			lastErr = fmt.Errorf("failed to start UDP listener on %q port %d: %w", bindAddr, port, err)
			if strings.Contains(err.Error(), "address already in use") {
				continue
			}
			return nil, lastErr
		}
		uc := pc.(*net.UDPConn)
		if err := setUDPRecvBuf(uc); err != nil {
			_ = tl.Close()
			_ = uc.Close()
			return nil, fmt.Errorf("failed to resize UDP buffer: %w", err)
		}
		tcpLn, udpConn = tl, uc
		break
	}
	if tcpLn == nil {
		return nil, lastErr
	}
	t := &reuseTransport{
		bindAddr:   bindAddr,
		logger:     logger,
		tcpLn:      tcpLn,
		udpConn:    udpConn,
		packetCh:   make(chan *memberlist.Packet),
		streamCh:   make(chan net.Conn),
		shutdownCh: make(chan struct{}),
	}
	t.wg.Add(2)
	go t.tcpListen()
	go t.udpListen()
	return t, nil
}

func (t *reuseTransport) GetAutoBindPort() int {
	return t.tcpLn.Addr().(*net.TCPAddr).Port
}

func (t *reuseTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	if ip != "" {
		addr := net.ParseIP(ip)
		if addr == nil {
			return nil, 0, fmt.Errorf("failed to parse advertise address %q", ip)
		}
		if ip4 := addr.To4(); ip4 != nil {
			addr = ip4
		}
		return addr, port, nil
	}
	boundIP := t.tcpLn.Addr().(*net.TCPAddr).IP
	if t.bindAddr == "0.0.0.0" || boundIP.IsUnspecified() {
		priv, err := sockaddr.GetPrivateIP()
		if err != nil {
			return nil, 0, fmt.Errorf("failed to get interface addresses: %w", err)
		}
		if priv == "" {
			return nil, 0, fmt.Errorf("no private IP address found, and explicit IP not provided")
		}
		addr := net.ParseIP(priv)
		if addr == nil {
			return nil, 0, fmt.Errorf("failed to parse advertise address %q", priv)
		}
		return addr, t.GetAutoBindPort(), nil
	}
	return boundIP, t.GetAutoBindPort(), nil
}

func (t *reuseTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	return t.WriteToAddress(b, memberlist.Address{Addr: addr})
}

func (t *reuseTransport) WriteToAddress(b []byte, a memberlist.Address) (time.Time, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", a.Addr)
	if err != nil {
		return time.Time{}, err
	}
	_, err = t.udpConn.WriteTo(b, udpAddr)
	return time.Now(), err
}

func (t *reuseTransport) PacketCh() <-chan *memberlist.Packet { return t.packetCh }

func (t *reuseTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialAddressTimeout(memberlist.Address{Addr: addr}, timeout)
}

func (t *reuseTransport) DialAddressTimeout(a memberlist.Address, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	return d.Dial("tcp", a.Addr)
}

func (t *reuseTransport) StreamCh() <-chan net.Conn { return t.streamCh }

func (t *reuseTransport) Shutdown() error {
	t.shutdownOnce.Do(func() {
		t.shutdown.Store(true)
		close(t.shutdownCh)
		_ = t.tcpLn.Close()
		_ = t.udpConn.Close()
	})
	t.wg.Wait()
	return nil
}

func (t *reuseTransport) tcpListen() {
	defer t.wg.Done()
	const (
		baseDelay = 5 * time.Millisecond
		maxDelay  = time.Second
	)
	var delay time.Duration
	for {
		conn, err := t.tcpLn.AcceptTCP()
		if err != nil {
			if t.shutdown.Load() {
				return
			}
			if delay == 0 {
				delay = baseDelay
			} else {
				delay = min(delay*2, maxDelay)
			}
			t.logger.Printf("[ERR] memberlist: Error accepting TCP connection: %v", err)
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-t.shutdownCh:
				timer.Stop()
				return
			}
			continue
		}
		delay = 0
		select {
		case t.streamCh <- conn:
		case <-t.shutdownCh:
			_ = conn.Close()
			return
		}
	}
}

func (t *reuseTransport) udpListen() {
	defer t.wg.Done()
	for {
		buf := make([]byte, udpPacketBufSize)
		n, addr, err := t.udpConn.ReadFrom(buf)
		ts := time.Now()
		if err != nil {
			if t.shutdown.Load() {
				return
			}
			t.logger.Printf("[ERR] memberlist: Error reading UDP packet: %v", err)
			continue
		}
		if n < 1 {
			t.logger.Printf("[ERR] memberlist: UDP packet too short (%d bytes) %s", n, addr)
			continue
		}
		select {
		case t.packetCh <- &memberlist.Packet{Buf: buf[:n], From: addr, Timestamp: ts}:
		case <-t.shutdownCh:
			return
		}
	}
}

func setUDPRecvBuf(c *net.UDPConn) error {
	size := udpRecvBufSize
	var err error
	for size > 0 {
		if err = c.SetReadBuffer(size); err == nil {
			return nil
		}
		size /= 2
	}
	return err
}
