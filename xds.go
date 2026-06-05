package channelz

import (
	"context"
	"fmt"
	"strings"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	csdsgrpc "github.com/envoyproxy/go-control-plane/envoy/service/status/v3"
	log "google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

const (
	virtualHostTypeURL = "type.googleapis.com/envoy.config.route.v3.VirtualHost"
	clusterTypeURL     = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	endpointTypeURL    = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
)

// parseXdsTarget extracts the authority and resource name from an xds:// target.
// gRPC-Go target format: xds:[//<authority>]/<resource>
// xds:///foo:443           → authority="", resource="foo:443"
// xds://my.authority/foo   → authority="my.authority", resource="foo"
func parseXdsTarget(target string) (authority, resource string, ok bool) {
	if !strings.HasPrefix(target, "xds:") {
		return "", "", false
	}
	rest := strings.TrimPrefix(target, "xds:")
	if !strings.HasPrefix(rest, "//") {
		// xds:foo — no authority section
		return "", strings.TrimPrefix(rest, "/"), true
	}
	rest = strings.TrimPrefix(rest, "//")
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return rest, "", true
	}
	return rest[:slash], rest[slash+1:], true
}

// fetchClientStatus calls CSDS FetchClientStatus and returns the (single) client
// config for the in-process xDS client, or nil if CSDS is unavailable / empty.
func (h *grpcChannelzHandler) fetchClientStatus(ctx context.Context) (*csdsgrpc.ClientConfig, error) {
	client, err := h.connectCSDS()
	if err != nil {
		return nil, err
	}
	resp, err := client.FetchClientStatus(ctx, &csdsgrpc.ClientStatusRequest{})
	if err != nil {
		return nil, err
	}
	if len(resp.GetConfig()) == 0 {
		return nil, nil
	}
	return resp.GetConfig()[0], nil
}

// xdsResources groups GenericXdsConfigs by typeUrl, keyed by resource name.
type xdsResources struct {
	virtualHosts map[string]*routev3.VirtualHost
	clusters     map[string]*clusterv3.Cluster
	endpoints    map[string]*endpointv3.ClusterLoadAssignment
}

func newXdsResources(cfg *csdsgrpc.ClientConfig) *xdsResources {
	r := &xdsResources{
		virtualHosts: map[string]*routev3.VirtualHost{},
		clusters:     map[string]*clusterv3.Cluster{},
		endpoints:    map[string]*endpointv3.ClusterLoadAssignment{},
	}
	if cfg == nil {
		return r
	}
	for _, gx := range cfg.GetGenericXdsConfigs() {
		any := gx.GetXdsConfig()
		if any == nil {
			continue
		}
		switch gx.GetTypeUrl() {
		case virtualHostTypeURL:
			m := &routev3.VirtualHost{}
			if err := any.UnmarshalTo(m); err == nil {
				r.virtualHosts[gx.GetName()] = m
			} else {
				log.Errorf("channelz: unmarshal VHDS %s: %v", gx.GetName(), err)
			}
		case clusterTypeURL:
			m := &clusterv3.Cluster{}
			if err := any.UnmarshalTo(m); err == nil {
				r.clusters[gx.GetName()] = m
			} else {
				log.Errorf("channelz: unmarshal CDS %s: %v", gx.GetName(), err)
			}
		case endpointTypeURL:
			m := &endpointv3.ClusterLoadAssignment{}
			if err := any.UnmarshalTo(m); err == nil {
				r.endpoints[gx.GetName()] = m
			} else {
				log.Errorf("channelz: unmarshal EDS %s: %v", gx.GetName(), err)
			}
		}
	}
	return r
}

// formatSocketAddress renders a core.SocketAddress as host:port.
func formatSocketAddress(sa *corev3.SocketAddress) string {
	if sa == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", sa.GetAddress(), sa.GetPortValue())
}

// formatLbEndpointAddress returns the host:port for the LbEndpoint's socket address, or "".
func formatLbEndpointAddress(ep *endpointv3.LbEndpoint) string {
	if ep == nil || ep.GetEndpoint() == nil || ep.GetEndpoint().GetAddress() == nil {
		return ""
	}
	return formatSocketAddress(ep.GetEndpoint().GetAddress().GetSocketAddress())
}

// edsServiceNameForCluster returns the EDS service name to look up in `endpoints`
// for a given cluster. Defaults to the cluster name when eds_cluster_config.service_name
// is not set.
func edsServiceNameForCluster(c *clusterv3.Cluster) string {
	if c == nil {
		return ""
	}
	if eds := c.GetEdsClusterConfig(); eds != nil && eds.GetServiceName() != "" {
		return eds.GetServiceName()
	}
	return c.GetName()
}

// protoToString renders a proto message as its textual form for diagnostics.
func protoToString(m proto.Message) string {
	if m == nil {
		return ""
	}
	b, err := prototext.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}
