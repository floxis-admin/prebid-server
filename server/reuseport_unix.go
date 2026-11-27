//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package server

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func setReusePort(network, address string, c syscall.RawConn) error {
	var sockErr error
	if err := c.Control(func(fd uintptr) {
		sockErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	}); err != nil {
		return err
	}
	return sockErr
}
