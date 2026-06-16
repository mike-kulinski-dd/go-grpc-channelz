package channelz

import (
	"context"
	"fmt"
	"io"
	"sort"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	upstreamshttpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	log "google.golang.org/grpc/grpclog"
)

// clusterMaxConcurrentStreams pulls HTTP/2 max_concurrent_streams off a CDS
// cluster. Modern Envoy/gRPC configs set it inside
// typed_extension_protocol_options["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"];
// older configs use the deprecated Cluster.http2_protocol_options field.
func clusterMaxConcurrentStreams(c *clusterv3.Cluster) (uint32, bool) {
	if c == nil {
		return 0, false
	}
	if tepo, ok := c.GetTypedExtensionProtocolOptions()["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"]; ok && tepo != nil {
		var opts upstreamshttpv3.HttpProtocolOptions
		if err := tepo.UnmarshalTo(&opts); err == nil {
			h2 := opts.GetExplicitHttpConfig().GetHttp2ProtocolOptions()
			if h2 == nil {
				h2 = opts.GetUseDownstreamProtocolConfig().GetHttp2ProtocolOptions()
			}
			if v := h2.GetMaxConcurrentStreams(); v != nil {
				return v.GetValue(), true
			}
		}
	}
	// nolint:staticcheck // intentional fallback to deprecated field for older configs
	if v := c.GetHttp2ProtocolOptions().GetMaxConcurrentStreams(); v != nil {
		return v.GetValue(), true
	}
	return 0, false
}

// poolRow is one address that one or more subchannels target.
type poolRow struct {
	Address        string
	Count          int
	SubchannelIDs  []int64
	CallsStarted   int64
	CallsSucceeded int64
	CallsFailed    int64
}

type poolsPageData struct {
	ChannelID int64
	Pools     []poolRow
}

type poolMemberRow struct {
	SubchannelID   int64
	Name           string
	State          string
	CallsStarted   int64
	CallsSucceeded int64
	CallsFailed    int64
	SocketIDs      []int64
}

type poolDetailPageData struct {
	ChannelID               int64
	Address                 string
	ClusterName             string
	MaxConcurrentStreams    uint32
	MaxConcurrentStreamsSet bool
	Members                 []poolMemberRow
}

// WriteSubchannelPoolsPage writes the per-channel list of subchannel pools (one row per remote address).
func (h *grpcChannelzHandler) WriteSubchannelPoolsPage(w io.Writer, channelID int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ subchannel pools (channel %d)", channelID))
	h.writeSubchannelPools(w, channelID)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeSubchannelPools(w io.Writer, channelID int64) {
	byAddr, info := h.subchannelsByAddress(channelID)
	d := &poolsPageData{ChannelID: channelID}
	for addr, ids := range byAddr {
		row := poolRow{
			Address:       addr,
			Count:         len(ids),
			SubchannelIDs: ids,
		}
		for _, id := range ids {
			si := info[id]
			row.CallsStarted += si.CallsStarted
			row.CallsSucceeded += si.CallsSucceeded
			row.CallsFailed += si.CallsFailed
		}
		d.Pools = append(d.Pools, row)
	}
	// Largest pools first, then by address for stable order.
	sort.Slice(d.Pools, func(i, j int) bool {
		if d.Pools[i].Count != d.Pools[j].Count {
			return d.Pools[i].Count > d.Pools[j].Count
		}
		return d.Pools[i].Address < d.Pools[j].Address
	})
	if err := subchannelPoolsTemplate.Execute(w, d); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

// WriteSubchannelPoolPage writes the detail page for one address — every subchannel pointing at it.
// clusterName is optional; when set, the page also surfaces the cluster's configured max_concurrent_streams.
func (h *grpcChannelzHandler) WriteSubchannelPoolPage(w io.Writer, channelID int64, address, clusterName string) {
	writeHeader(w, fmt.Sprintf("ChannelZ subchannel pool %s (channel %d)", address, channelID))
	h.writeSubchannelPool(w, channelID, address, clusterName)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeSubchannelPool(w io.Writer, channelID int64, address, clusterName string) {
	byAddr, info := h.subchannelsByAddress(channelID)
	d := &poolDetailPageData{ChannelID: channelID, Address: address, ClusterName: clusterName}

	if clusterName != "" {
		if cfg, err := h.fetchClientStatus(context.Background()); err == nil {
			if cluster := newXdsResources(cfg).clusters[clusterName]; cluster != nil {
				if v, ok := clusterMaxConcurrentStreams(cluster); ok {
					d.MaxConcurrentStreams = v
					d.MaxConcurrentStreamsSet = true
				}
			}
		}
	}
	for _, id := range byAddr[address] {
		si, ok := info[id]
		if !ok {
			continue
		}
		d.Members = append(d.Members, poolMemberRow{
			SubchannelID:   id,
			Name:           si.Name,
			State:          si.State,
			CallsStarted:   si.CallsStarted,
			CallsSucceeded: si.CallsSucceeded,
			CallsFailed:    si.CallsFailed,
			SocketIDs:      si.SocketIDs,
		})
	}
	sort.Slice(d.Members, func(i, j int) bool {
		return d.Members[i].SubchannelID < d.Members[j].SubchannelID
	})
	if err := subchannelPoolTemplate.Execute(w, d); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

const subchannelPoolsTemplateHTML = `
<p><a href="{{link "channel" .ChannelID}}">&laquo; back to channel {{.ChannelID}}</a></p>
<p>Subchannels grouped by their remote address. Pools with more than one subchannel indicate that the dynamic-scaling path opened additional connections to the same IP (e.g. on MAX_CONCURRENT_STREAMS).</p>
{{if .Pools}}
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>Address</th>
		<th># Subchannels</th>
		<th>Subchannels</th>
	</tr>
	{{range .Pools}}
	<tr>
		<td><a href="{{link "channel" $.ChannelID "pool"}}?addr={{.Address | urlquery}}">{{.Address}}</a></td>
		<td>{{.Count}}</td>
		<td>
			{{range .SubchannelIDs}}
				<a href="{{link "subchannel" .}}"><b>{{.}}</b></a>
			{{end}}
		</td>
	</tr>
	{{end}}
</table>
{{else}}
<p><i>No subchannels found for this channel.</i></p>
{{end}}
`

const subchannelPoolTemplateHTML = `
<p>
	<a href="{{link "channel" .ChannelID}}">&laquo; back to channel {{.ChannelID}}</a>
	&nbsp;|&nbsp;
	<a href="{{link "channel" .ChannelID "pools"}}">all pools</a>
</p>
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Channel</th><td>{{.ChannelID}}</td></tr>
	<tr><th>Address</th><td>{{.Address}}</td></tr>
	<tr><th># Subchannels</th><td>{{len .Members}}</td></tr>
	{{if .ClusterName}}<tr><th>Cluster</th><td>{{.ClusterName}}</td></tr>{{end}}
	<tr><th>max_concurrent_streams</th><td>
		{{- if .MaxConcurrentStreamsSet}}{{.MaxConcurrentStreams}}
		{{- else if .ClusterName}}<i>(unset on cluster {{.ClusterName}})</i>
		{{- else}}<i>(unknown — no cluster context)</i>
		{{- end}}
	</td></tr>
</table>
{{if .Members}}
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>Subchannel</th>
		<th>Name</th>
		<th>State</th>
		<th>Calls started</th>
		<th>Calls failed</th>
		<th>Socket(s)</th>
	</tr>
	{{range .Members}}
	<tr>
		<td><a href="{{link "subchannel" .SubchannelID}}"><b>{{.SubchannelID}}</b></a></td>
		<td>{{.Name}}</td>
		<td>{{.State}}</td>
		<td>{{.CallsStarted}}</td>
		<td>{{.CallsFailed}}</td>
		<td>
			{{range .SocketIDs}}
				<a href="{{link "socket" .}}">{{.}}</a>
			{{end}}
		</td>
	</tr>
	{{end}}
</table>
{{else}}
<p><i>No subchannels currently targeting this address.</i></p>
{{end}}
`
