package raftlib

import (
	"errors"
	"net"
	"strings"
)

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func getIP(peerAddress string) (string, error) {
	tmp := peerAddress
	if idx := strings.Index(tmp, "://"); idx != -1 && idx+3 < len(tmp) {
		tmp = tmp[idx+3:]
	}
	splits := strings.Split(tmp, ":")
	if len(splits) != 2 {
		return "", errors.New("peer address format error: " + peerAddress)
	}
	return splits[0], nil
}
