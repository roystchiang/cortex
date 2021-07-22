package memcache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/thanos-io/thanos/pkg/errutil"
	"net"
	"strconv"
	"strings"
	"time"
)

type Provider struct {
	dialTimeout			time.Duration
}

type ClusterConfig struct {
	version uint64
	nodes []Node
}

type Node struct {
	dns 	string
	ip		string
	port	int
}

func NewProvider(dialTimeout time.Duration) *Provider {
	p := &Provider{
		dialTimeout: dialTimeout,
	}
	return p
}

func (p *Provider) Resolve(ctx context.Context , addressess []string) error {
	errs := errutil.MultiError{}
	for _, address := range addressess {
		fmt.Printf("Resolving %s\n", address)
		conn, err := net.DialTimeout("tcp", address, p.dialTimeout)
		if err != nil {
			errs.Add(err)
			continue
		}

		rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		if _, err := fmt.Fprintf(rw, "config get cluster\n"); err != nil {
			errs.Add(err)
			continue
		}
		if err := rw.Flush(); err != nil {
			errs.Add(err)
			continue
		}

		clusterConfig, err :=p.parseConfig(rw.Reader)
		if err != nil {
			errs.Add(err)
			continue
		}
		fmt.Printf("clusterConfig: %+v\n", clusterConfig)
		if err := conn.Close(); err != nil {
			errs.Add(err)
			continue
		}
	}

	return errs.Err()
}

func (p *Provider) parseConfig(reader*bufio.Reader) (*ClusterConfig, error) {
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
	nodes, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if len(configVersion) + len(nodes) != configSize {
		return nil, errors.New(fmt.Sprintf("expected %i in config payload, but got %i instead. version: %s, nodes: %s", configSize, len(configVersion) + len(nodes), configVersion, nodes))
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

func (p *Provider) Addresses() []string {
	var result []string
	return result
}
