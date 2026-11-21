//go:build !unix

package cluster

import "syscall"

func reuseControl(network, address string, c syscall.RawConn) error {
	return nil
}
