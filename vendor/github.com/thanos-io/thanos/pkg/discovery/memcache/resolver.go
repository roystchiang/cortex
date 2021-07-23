package memcache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type memcachedAutoDiscovery struct {
	dialTimeout				time.Duration
}


func (s *memcachedAutoDiscovery) Resolve(ctx context.Context, address string) (*ClusterConfig, error) {
	conn, err := net.DialTimeout("tcp", address, s.dialTimeout)

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	if _, err := fmt.Fprintf(rw, "config get cluster\n"); err != nil {
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		return nil, err
	}

	clusterConfig, err := s.parseConfig(rw.Reader)
	if err != nil {
		return nil, err
	}

	if err := conn.Close(); err != nil {
		return nil, err
	}
	return clusterConfig, nil
}

func (s *memcachedAutoDiscovery) parseConfig(reader*bufio.Reader) (*ClusterConfig, error) {
	clusterConfig := new(ClusterConfig)

	configMeta, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	// First line should be "CONFIG cluster 0 [length-of-payload-]
	configSize, err := strconv.Atoi(strings.Split(strings.TrimSpace(configMeta), " ")[3])
	if err != nil {
		return nil, err
	}

	configVersion, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	clusterConfig.version, err= strconv.Atoi(strings.TrimSpace(configVersion))

	nodes, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if len(configVersion) + len(nodes) != configSize {
		return nil, errors.New(fmt.Sprintf("expected %d in config payload, but got %d instead. version: %s, nodes: %s", configSize, len(configVersion) + len(nodes), configVersion, nodes))
	}


	for _, host := range strings.Split(strings.TrimSpace(nodes), " ") {
		dnsIpPort := strings.Split(host, "|")
		if len(dnsIpPort) != 3 {
			return nil, errors.New(fmt.Sprintf("host not in expected format: %s", dnsIpPort))
		}
		port, err := strconv.Atoi(dnsIpPort[2])
		if err != nil {
			return nil, err
		}
		clusterConfig.nodes = append(clusterConfig.nodes, Node{dns: dnsIpPort[0], ip: dnsIpPort[1], port: port})
	}

	return clusterConfig, nil
}
