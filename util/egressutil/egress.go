package egressutil

import (
	"fmt"
	"net/netip"
	"syscall"
)

var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")
var thisNetwork = netip.MustParsePrefix("0.0.0.0/8")

// PublicOnlyControl runs after DNS resolution, so a name that resolves to an internal address is refused too.
func PublicOnlyControl(network, address string, _ syscall.RawConn) error {
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("egress refused: unparseable destination %q: %w", address, err)
	}
	if !IsPublicAddr(addrPort.Addr()) {
		return fmt.Errorf("egress refused: %s is not a public address", addrPort.Addr())
	}
	return nil
}

// IsPublicAddr reports whether addr is a globally routable unicast address.
func IsPublicAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() {
		return false
	}
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsUnspecified() ||
		addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsInterfaceLocalMulticast() || addr.IsMulticast() {
		return false
	}
	if addr.Is4() && (sharedAddressSpace.Contains(addr) || thisNetwork.Contains(addr)) {
		return false
	}
	return true
}
