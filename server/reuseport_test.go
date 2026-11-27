//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package server

import (
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTCPListenerReusePort(t *testing.T) {
	first, err := newTCPListener("127.0.0.1:0", true, nil)
	require.NoError(t, err)
	defer first.Close()
	addr := first.Addr().String()

	second, err := newTCPListener(addr, true, nil)
	require.NoError(t, err, "a replacement must bind the port the running process holds")
	second.Close()

	_, err = newTCPListener(addr, false, nil)
	assert.Error(t, err, "without socket_reuse_port a second bind must still fail")
}

func TestNotifyReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	require.NoError(t, err)
	defer conn.Close()

	t.Setenv("NOTIFY_SOCKET", path)
	notifyReady()

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "READY=1", string(buf[:n]))
}
