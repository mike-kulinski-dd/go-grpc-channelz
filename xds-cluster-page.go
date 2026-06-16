package channelz

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	csdsgrpc "github.com/envoyproxy/go-control-plane/envoy/service/status/v3"
	log "google.golang.org/grpc/grpclog"
)

// strictDNSPorts collects the ports used by STRICT_DNS / LOGICAL_DNS clusters'
// inline LoadAssignments. DNS clusters don't appear in EDS (endpoints are
// resolved client-side by hostname), so we can't match subchannels to them
// by IP — but the port survives DNS resolution, and is generally distinctive
// enough to filter sibling-cluster subchannels out of the orphan list.
func strictDNSPorts(clusters map[string]*clusterv3.Cluster) map[uint32]bool {
	out := map[uint32]bool{}
	for _, c := range clusters {
		t := c.GetType()
		if t != clusterv3.Cluster_STRICT_DNS && t != clusterv3.Cluster_LOGICAL_DNS {
			continue
		}
		for _, lle := range c.GetLoadAssignment().GetEndpoints() {
			for _, lb := range lle.GetLbEndpoints() {
				if p := lb.GetEndpoint().GetAddress().GetSocketAddress().GetPortValue(); p != 0 {
					out[p] = true
				}
			}
		}
	}
	return out
}

// portFromHostPort returns the port from a "host:port" target string, or 0.
func portFromHostPort(target string) uint32 {
	_, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return 0
	}
	p, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(p)
}

// WriteXdsClusterPage writes the CDS + EDS view for a cluster referenced by an xDS channel,
// joined with the channel's channelz subchannels by address.
func (h *grpcChannelzHandler) WriteXdsClusterPage(w io.Writer, channelID int64, clusterName string) {
	writeHeader(w, fmt.Sprintf("xDS Cluster %s (channel %d)", clusterName, channelID))
	d := h.getXdsCluster(channelID, clusterName)
	if err := xdsClusterTemplate.Execute(w, d); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
	writeRawDump(w,
		rawDumpSection{Title: "Node", Msg: d.Node},
		rawDumpSection{Title: "Cluster (CDS)", Msg: d.Cluster},
		rawDumpSection{Title: "ClusterLoadAssignment (EDS)", Msg: d.Assignment},
	)
	writeFooter(w)
}

type endpointRow struct {
	Address        string
	HealthStatus   corev3.HealthStatus
	Weight         uint32
	Locality       string
	SubchannelID   int64 // 0 means no match
	SubchannelName string
	PoolSize       int // number of subchannels for this address; >1 means a duplicate pool
}

type localityGroup struct {
	Locality string
	Priority uint32
	Weight   uint32
	Rows     []endpointRow
}

type unmatchedSubchannelRow struct {
	SubchannelID int64
	Name         string
	Target       string
}

type xdsClusterPageData struct {
	ChannelID            int64
	ClusterName          string
	Node                 *corev3.Node
	Cluster              *clusterv3.Cluster
	IsDNSCluster         bool
	EDSServiceName       string
	Assignment           *endpointv3.ClusterLoadAssignment
	Localities           []localityGroup
	Pools                []poolRow
	UnmatchedSubchannels []unmatchedSubchannelRow
	Error                string
}

func (h *grpcChannelzHandler) getXdsCluster(channelID int64, clusterName string) *xdsClusterPageData {
	d := &xdsClusterPageData{ChannelID: channelID, ClusterName: clusterName}

	// Fetch CSDS config and the channel's subchannels in parallel — they're
	// independent RPCs (and the subchannel fan-out below is the slow part).
	var (
		cfg       *csdsgrpc.ClientConfig
		cfgErr    error
		subByAddr map[string][]int64
		subInfo   map[int64]subchannelInfo
		wg        sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cfg, cfgErr = h.fetchClientStatus(context.Background())
	}()
	go func() {
		defer wg.Done()
		subByAddr, subInfo = h.subchannelsByAddress(channelID)
	}()
	wg.Wait()

	if cfgErr != nil {
		d.Error = fmt.Sprintf("CSDS FetchClientStatus failed: %v", cfgErr)
		return d
	}
	d.Node = cfg.GetNode()
	resources := newXdsResources(cfg)

	cluster, ok := resources.clusters[clusterName]
	if !ok {
		d.Error = fmt.Sprintf("no CDS resource named %q in CSDS dump", clusterName)
		return d
	}
	d.Cluster = cluster
	d.EDSServiceName = edsServiceNameForCluster(cluster)
	ct := cluster.GetType()
	d.IsDNSCluster = ct == clusterv3.Cluster_STRICT_DNS || ct == clusterv3.Cluster_LOGICAL_DNS
	if d.IsDNSCluster {
		// DNS clusters don't appear in EDS — endpoints live inline on the CDS
		// resource as hostname:port pairs that get resolved client-side.
		d.Assignment = cluster.GetLoadAssignment()
	} else {
		d.Assignment = resources.endpoints[d.EDSServiceName]
	}

	// For DNS clusters, collect this cluster's ports so we can attribute
	// subchannels to it by port (we don't have IPs to match against).
	thisClusterPorts := map[uint32]bool{}
	if d.IsDNSCluster {
		for _, lle := range cluster.GetLoadAssignment().GetEndpoints() {
			for _, lb := range lle.GetLbEndpoints() {
				if p := lb.GetEndpoint().GetAddress().GetSocketAddress().GetPortValue(); p != 0 {
					thisClusterPorts[p] = true
				}
			}
		}
	}

	matched := map[int64]bool{}
	if d.IsDNSCluster {
		for id, info := range subInfo {
			if thisClusterPorts[portFromHostPort(info.Target)] {
				matched[id] = true
			}
		}
	}
	if d.Assignment != nil {
		for _, lle := range d.Assignment.GetEndpoints() {
			group := localityGroup{
				Locality: formatLocality(lle.GetLocality()),
				Priority: lle.GetPriority(),
				Weight:   lle.GetLoadBalancingWeight().GetValue(),
			}
			for _, ep := range lle.GetLbEndpoints() {
				addr := formatLbEndpointAddress(ep)
				row := endpointRow{
					Address:      addr,
					HealthStatus: ep.GetHealthStatus(),
					Weight:       ep.GetLoadBalancingWeight().GetValue(),
					Locality:     group.Locality,
				}
				if ids, ok := subByAddr[addr]; ok && len(ids) > 0 {
					row.SubchannelID = ids[0]
					row.SubchannelName = subInfo[ids[0]].Name
					row.PoolSize = len(ids)
					for _, id := range ids {
						matched[id] = true
					}
				}
				group.Rows = append(group.Rows, row)
			}
			d.Localities = append(d.Localities, group)
		}
	}

	// Addresses owned by sibling clusters this same xDS channel watches.
	// Includes (a) addresses in any EDS resource, and (b) DNS-resolved IPs
	// for STRICT_DNS / LOGICAL_DNS clusters whose endpoints don't show up in
	// EDS at all. A subchannel whose target is in this set is not a true
	// orphan — it just belongs to a sibling cluster.
	siblingAddrs := map[string]bool{}
	for _, ep := range resources.endpoints {
		for _, lle := range ep.GetEndpoints() {
			for _, lb := range lle.GetLbEndpoints() {
				if addr := formatLbEndpointAddress(lb); addr != "" {
					siblingAddrs[addr] = true
				}
			}
		}
	}
	dnsPorts := strictDNSPorts(resources.clusters)
	for id, info := range subInfo {
		if matched[id] {
			continue
		}
		if siblingAddrs[info.Target] {
			continue
		}
		if dnsPorts[portFromHostPort(info.Target)] {
			continue
		}
		d.UnmatchedSubchannels = append(d.UnmatchedSubchannels, unmatchedSubchannelRow{
			SubchannelID: id,
			Name:         info.Name,
			Target:       info.Target,
		})
	}
	sort.Slice(d.UnmatchedSubchannels, func(i, j int) bool {
		return d.UnmatchedSubchannels[i].SubchannelID < d.UnmatchedSubchannels[j].SubchannelID
	})

	// Pool view: every subchannel attributed to this cluster (DNS or EDS),
	// grouped by remote address. A pool with more than one subchannel means
	// the dynamic-scaling path opened additional connections to the same IP.
	pools := map[string][]int64{}
	for id := range matched {
		info, ok := subInfo[id]
		if !ok {
			continue
		}
		pools[info.Target] = append(pools[info.Target], id)
	}
	for addr, ids := range pools {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		row := poolRow{
			Address:       addr,
			Count:         len(ids),
			SubchannelIDs: ids,
		}
		for _, id := range ids {
			si := subInfo[id]
			row.CallsStarted += si.CallsStarted
			row.CallsSucceeded += si.CallsSucceeded
			row.CallsFailed += si.CallsFailed
		}
		d.Pools = append(d.Pools, row)
	}
	sort.Slice(d.Pools, func(i, j int) bool {
		if d.Pools[i].Count != d.Pools[j].Count {
			return d.Pools[i].Count > d.Pools[j].Count
		}
		return d.Pools[i].Address < d.Pools[j].Address
	})
	return d
}

type subchannelInfo struct {
	Name           string
	Target         string
	State          string
	CallsStarted   int64
	CallsSucceeded int64
	CallsFailed    int64
	SocketIDs      []int64
}

// subchannelsByAddress builds a map of address (host:port) → subchannel IDs
// (slice — multiple subchannels can share an address once the dynamic-scaling
// path opens additional connections past MAX_CONCURRENT_STREAMS), plus an info
// map keyed by subchannel ID, for every subchannel of the channel.
func (h *grpcChannelzHandler) subchannelsByAddress(channelID int64) (map[string][]int64, map[int64]subchannelInfo) {
	byAddr := map[string][]int64{}
	info := map[int64]subchannelInfo{}

	ch := h.getChannel(channelID)
	if ch == nil || ch.GetChannel() == nil {
		return byAddr, info
	}
	refs := ch.GetChannel().GetSubchannelRef()

	// Fan out the per-subchannel GetSubchannel RPCs. The shared gRPC client
	// multiplexes them on a single HTTP/2 connection, so a bounded worker
	// pool turns N sequential round trips into ~N/parallelism.
	const parallelism = 32
	workers := parallelism
	if len(refs) < workers {
		workers = len(refs)
	}
	type result struct {
		id   int64
		info subchannelInfo
		ok   bool
	}
	in := make(chan int64, len(refs))
	out := make(chan result, len(refs))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range in {
				sub := h.getSubchannel(id)
				if sub == nil || sub.GetSubchannel() == nil {
					continue
				}
				s := sub.GetSubchannel()
				si := subchannelInfo{
					Name:           s.GetRef().GetName(),
					Target:         s.GetData().GetTarget(),
					State:          s.GetData().GetState().GetState().String(),
					CallsStarted:   s.GetData().GetCallsStarted(),
					CallsSucceeded: s.GetData().GetCallsSucceeded(),
					CallsFailed:    s.GetData().GetCallsFailed(),
				}
				for _, sr := range s.GetSocketRef() {
					si.SocketIDs = append(si.SocketIDs, sr.GetSocketId())
				}
				out <- result{
					id:   s.GetRef().GetSubchannelId(),
					info: si,
					ok:   true,
				}
			}
		}()
	}
	for _, ref := range refs {
		in <- ref.GetSubchannelId()
	}
	close(in)
	go func() { wg.Wait(); close(out) }()
	for r := range out {
		if !r.ok {
			continue
		}
		info[r.id] = r.info
		byAddr[r.info.Target] = append(byAddr[r.info.Target], r.id)
	}
	for addr := range byAddr {
		sort.Slice(byAddr[addr], func(i, j int) bool { return byAddr[addr][i] < byAddr[addr][j] })
	}
	return byAddr, info
}

func formatLocality(l *corev3.Locality) string {
	if l == nil {
		return ""
	}
	parts := []string{}
	if l.GetRegion() != "" {
		parts = append(parts, l.GetRegion())
	}
	if l.GetZone() != "" {
		parts = append(parts, l.GetZone())
	}
	if l.GetSubZone() != "" {
		parts = append(parts, l.GetSubZone())
	}
	if len(parts) == 0 {
		return "(default)"
	}
	return joinSlash(parts)
}

func joinSlash(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "/"
		}
		out += p
	}
	return out
}

const xdsClusterTemplateHTML = `
{{if .Error}}
	<p><b>Error:</b> {{.Error}}</p>
{{end}}
<p>
	<a href="{{link "channel" .ChannelID}}">&laquo; back to channel {{.ChannelID}}</a>
	&nbsp;|&nbsp;
	<a href="{{link "channel" .ChannelID "pools"}}">view subchannel pools</a>
</p>
{{if .Cluster}}
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Cluster name</th><td>{{.Cluster.Name}}</td></tr>
	<tr><th>Type</th><td>{{.Cluster.GetType}}</td></tr>
	<tr><th>LB policy</th><td>{{.Cluster.GetLbPolicy}}</td></tr>
	<tr><th>EDS service</th><td>{{.EDSServiceName}}</td></tr>
	{{with .Cluster.LrsServer}}<tr><th>LRS server</th><td>configured</td></tr>{{end}}
</table>

<h3>Subchannel pools</h3>
<p>Subchannels attributed to this cluster, grouped by remote address. A pool of more than one subchannel indicates the dynamic-scaling path opened additional connections to the same IP.</p>
{{if .Pools}}
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>Address</th>
		<th># Subchannels</th>
		<th>Calls started</th>
		<th>Calls failed</th>
	</tr>
	{{range .Pools}}
	<tr>
		<td><a href="{{link "channel" $.ChannelID "pool"}}?addr={{.Address | urlquery}}&cluster={{$.ClusterName | urlquery}}">{{.Address}}</a></td>
		<td>{{.Count}}</td>
		<td>{{.CallsStarted}}</td>
		<td>{{.CallsFailed}}</td>
	</tr>
	{{end}}
</table>
{{else}}
<p><i>No subchannels attributed to this cluster.</i></p>
{{end}}

{{if .IsDNSCluster}}
<h3>Configured DNS endpoints (from CDS load_assignment)</h3>
{{if not .Localities}}
<p><i>No inline endpoints configured.</i></p>
{{end}}
{{else}}
<h3>Endpoints (from EDS)</h3>
{{end}}
{{if .Localities}}
{{range .Localities}}
<h4>Locality: {{.Locality}} (priority {{.Priority}}, weight {{.Weight}})</h4>
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>Address</th>
		<th>Health</th>
		<th>Weight</th>
		<th>Channelz subchannel</th>
	</tr>
	{{range .Rows}}
	<tr>
		<td>{{.Address}}</td>
		<td>{{.HealthStatus}}</td>
		<td>{{.Weight}}</td>
		<td>
			{{if .SubchannelID}}
				<a href="{{link "subchannel" .SubchannelID}}"><b>{{.SubchannelID}}</b> {{.SubchannelName}}</a>
				{{if gt .PoolSize 1}}
					&nbsp;<a href="{{link "channel" $.ChannelID "pool"}}?addr={{.Address | urlquery}}&cluster={{$.ClusterName | urlquery}}">({{.PoolSize}} in pool)</a>
				{{end}}
			{{else}}
				&mdash;
			{{end}}
		</td>
	</tr>
	{{end}}
</table>
{{end}}
{{else if not .IsDNSCluster}}
<p><i>No EDS endpoints found for service name {{.EDSServiceName}}.</i></p>
{{end}}

{{if .UnmatchedSubchannels}}
<h3>Unmatched subchannels</h3>
<p>Subchannels of channel {{.ChannelID}} whose address is not in the current EDS dump.</p>
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header"><th>Subchannel</th><th>Target</th></tr>
	{{range .UnmatchedSubchannels}}
	<tr>
		<td><a href="{{link "subchannel" .SubchannelID}}"><b>{{.SubchannelID}}</b> {{.Name}}</a></td>
		<td>{{.Target}}</td>
	</tr>
	{{end}}
</table>
{{end}}
{{end}}
`
