package rapidash

import (
	"net"
	"strings"

	"golang.org/x/xerrors"
)

func getAddr(server string) (net.Addr, error) {
	if strings.Contains(server, "/") {
		addr, err := net.ResolveUnixAddr("unix", server)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve unix addr %s: %w", server, err)
		}
		return addr, nil
	}
	tcpaddr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve tcp addr %s: %w", server, err)
	}
	return tcpaddr, nil
}
