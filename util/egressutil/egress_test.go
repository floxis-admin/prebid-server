package egressutil

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPublicAddr(t *testing.T) {
	refused := []string{
		"127.0.0.1", "127.8.9.10", "::1",
		"10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.31.17",
		"169.254.169.254", "fe80::1",
		"100.64.0.1", "100.127.255.254",
		"0.0.0.0", "0.1.2.3", "::",
		"224.0.0.1", "ff02::1",
		"fc00::1", "fd12:3456::1",
		"::ffff:127.0.0.1", "::ffff:192.168.31.17",
	}
	for _, s := range refused {
		assert.False(t, IsPublicAddr(netip.MustParseAddr(s)), s)
	}

	allowed := []string{"8.8.8.8", "1.1.1.1", "185.150.189.51", "100.63.255.255", "100.128.0.1", "172.32.0.1", "2606:4700::1111"}
	for _, s := range allowed {
		assert.True(t, IsPublicAddr(netip.MustParseAddr(s)), s)
	}

	assert.False(t, IsPublicAddr(netip.Addr{}))
}

func TestPublicOnlyControlRejectsUnparseable(t *testing.T) {
	assert.Error(t, PublicOnlyControl("tcp", "not-an-address", nil))
	assert.NoError(t, PublicOnlyControl("tcp", "8.8.8.8:443", nil))
	assert.Error(t, PublicOnlyControl("tcp", "127.0.0.1:6060", nil))
	assert.Error(t, PublicOnlyControl("tcp6", "[::1]:6060", nil))
}

func TestDialerWithPublicOnlyControlNeverReachesLoopback(t *testing.T) {
	hit := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
	}))
	defer server.Close()

	client := &http.Client{Transport: &http.Transport{
		DialContext: (&net.Dialer{Control: PublicOnlyControl}).DialContext,
	}}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/openrtb2?sid=1", nil)
	_, err := client.Do(req)

	assert.ErrorContains(t, err, "egress refused")
	assert.False(t, hit)
}
