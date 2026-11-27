//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package server

import (
	"fmt"
	"syscall"
)

func setReusePort(network, address string, c syscall.RawConn) error {
	return fmt.Errorf("socket_reuse_port is not supported on this platform")
}
