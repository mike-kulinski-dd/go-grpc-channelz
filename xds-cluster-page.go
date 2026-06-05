package channelz

import (
	"context"
	"fmt"
	"io"
	"sort"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	log "google.golang.org/grpc/grpclog"
)

// WriteXdsClusterPage writes the CDS + EDS view for a cluster referenced by an xDS channel,
// joined with the channel's channelz subchannels by address.
func (h *grpcChannelzHandler) WriteXdsClusterPage(w io.Writer, channelID int64, clusterName string) {
	writeHeader(w, fmt.Sprintf("xDS Cluster %s (channel %d)", clusterName, channelID))
	h.writeXdsCluster(w, channelID, clusterName)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeXdsCluster(w io.Writer, channelID int64, clusterName string) {
	d := h.getXdsCluster(channelID, clusterName)
	if err := xdsClusterTemplate.Execute(w, d); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

type endpointRow struct {
	Address        string
	HealthStatus   corev3.HealthStatus
	Weight         uint32
	Locality       string
	SubchannelID   int64 // 0 means no match
	SubchannelName string
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
	Cluster              *clusterv3.Cluster
	EDSServiceName       string
	Assignment           *endpointv3.ClusterLoadAssignment
	Localities           []localityGroup
	UnmatchedSubchannels []unmatchedSubchannelRow
	Error                string
}

func (h *grpcChannelzHandler) getXdsCluster(channelID int64, clusterName string) *xdsClusterPageData {
	d := &xdsClusterPageData{ChannelID: channelID, ClusterName: clusterName}

	cfg, err := h.fetchClientStatus(context.Background())
	if err != nil {
		d.Error = fmt.Sprintf("CSDS FetchClientStatus failed: %v", err)
		return d
	}
	resources := newXdsResources(cfg)

	cluster, ok := resources.clusters[clusterName]
	if !ok {
		d.Error = fmt.Sprintf("no CDS resource named %q in CSDS dump", clusterName)
		return d
	}
	d.Cluster = cluster
	d.EDSServiceName = edsServiceNameForCluster(cluster)
	d.Assignment = resources.endpoints[d.EDSServiceName]

	// Build addr → subchannel map for the channel.
	subByAddr, subInfo := h.subchannelsByAddress(channelID)

	matched := map[int64]bool{}
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
				if id, ok := subByAddr[addr]; ok {
					row.SubchannelID = id
					row.SubchannelName = subInfo[id].Name
					matched[id] = true
				}
				group.Rows = append(group.Rows, row)
			}
			d.Localities = append(d.Localities, group)
		}
	}

	for id, info := range subInfo {
		if matched[id] {
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
	return d
}

type subchannelInfo struct {
	Name   string
	Target string
}

// subchannelsByAddress builds a map of address (host:port) → subchannel ID,
// plus an info map keyed by subchannel ID, for every subchannel of the channel.
func (h *grpcChannelzHandler) subchannelsByAddress(channelID int64) (map[string]int64, map[int64]subchannelInfo) {
	byAddr := map[string]int64{}
	info := map[int64]subchannelInfo{}

	ch := h.getChannel(channelID)
	if ch == nil || ch.GetChannel() == nil {
		return byAddr, info
	}
	for _, ref := range ch.GetChannel().GetSubchannelRef() {
		sub := h.getSubchannel(ref.GetSubchannelId())
		if sub == nil || sub.GetSubchannel() == nil {
			continue
		}
		s := sub.GetSubchannel()
		id := s.GetRef().GetSubchannelId()
		info[id] = subchannelInfo{Name: s.GetRef().GetName(), Target: s.GetData().GetTarget()}
		byAddr[s.GetData().GetTarget()] = id
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
<p><a href="{{link "channel" .ChannelID}}">&laquo; back to channel {{.ChannelID}}</a></p>
{{if .Cluster}}
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Cluster name</th><td>{{.Cluster.Name}}</td></tr>
	<tr><th>Type</th><td>{{.Cluster.Type}}</td></tr>
	<tr><th>LB policy</th><td>{{.Cluster.LbPolicy}}</td></tr>
	<tr><th>EDS service</th><td>{{.EDSServiceName}}</td></tr>
	{{with .Cluster.LrsServer}}<tr><th>LRS server</th><td>configured</td></tr>{{end}}
</table>

<h3>Endpoints (from EDS)</h3>
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
			{{else}}
				&mdash;
			{{end}}
		</td>
	</tr>
	{{end}}
</table>
{{end}}
{{else}}
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
