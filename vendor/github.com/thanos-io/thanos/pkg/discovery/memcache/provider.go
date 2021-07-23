package memcache

import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
	"sync"
	"time"
)

type Provider struct {
	sync.RWMutex
	resolver				memcachedAutoDiscovery
	clusterConfigs			map[string]*ClusterConfig
	logger					log.Logger

	configVersion			prometheus.Gauge
	resolvedAddresses		*extprom.TxGaugeVec
	resolverFailuresCount	prometheus.Counter
	resolverLookupsCount	prometheus.Counter
}

type ClusterConfig struct {
	version int
	nodes []Node
}

type Node struct {
	dns 	string
	ip		string
	port	int
}

func NewProvider(logger log.Logger, reg prometheus.Registerer, dialTimeout time.Duration) *Provider {
	p := &Provider{
		resolver: memcachedAutoDiscovery{dialTimeout: dialTimeout},
		clusterConfigs: map[string]*ClusterConfig{},
		configVersion: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "auto_discovery_config_version",
			Help: "The current auto discovery config version",
		}),
		resolvedAddresses: extprom.NewTxGaugeVec(reg, prometheus.GaugeOpts{
			Name: "auto_discovery_resolved_addresses",
			Help: "The number of memcached nodes found via auto discovery",
		}, []string{"addr"}),
		resolverLookupsCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "auto_discovery_total",
			Help: "The number of memcache auto discovery attempts",
		}),
		resolverFailuresCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "auto_discovery_failures_total",
			Help: "The number of memcache auto discovery failures",
		}),
	}
	return p
}

func (p *Provider) Resolve(ctx context.Context , addresses []string) error {
	clusterConfigs := map[string]*ClusterConfig{}
	errs := errutil.MultiError{}

	for _, address := range addresses {
		clusterConfig, err := p.resolver.Resolve(ctx, address)
		p.resolverLookupsCount.Inc()

		if err != nil {
			errs.Add(err)
			p.resolverFailuresCount.Inc()
		} else {
			clusterConfigs[address] = clusterConfig
		}
	}

	p.Lock()
	defer p.Unlock()

	p.resolvedAddresses.ResetTx()
	for address, config := range clusterConfigs {
		p.resolvedAddresses.WithLabelValues(address).Set(float64(len(config.nodes)))
	}
	p.resolvedAddresses.Submit()

	p.clusterConfigs = clusterConfigs

	return errs.Err()
}

func (p *Provider) Addresses() []string {
	var result []string
	for _, config := range p.clusterConfigs {
		for _, node := range config.nodes {
			result = append(result, fmt.Sprintf("%s:%d", node.dns, node.port))
		}
	}
	return result
}
