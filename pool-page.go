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

const subchannelPoolTemplateHTML = `
<p><a href="{{link "channel" .ChannelID}}">&laquo; back to channel {{.ChannelID}}</a></p>
<table class="detail">
	<tr><th>Channel</th><td class="mono">{{.ChannelID}}</td></tr>
	<tr><th>Address</th><td class="mono">{{.Address}}</td></tr>
	<tr><th># Subchannels</th><td class="num">{{len .Members}}</td></tr>
	{{if .ClusterName}}<tr><th>Cluster</th><td class="mono">{{.ClusterName}}</td></tr>{{end}}
	<tr><th>max_concurrent_streams</th><td>
		{{- if .MaxConcurrentStreamsSet}}{{.MaxConcurrentStreams}}
		{{- else if .ClusterName}}<i>(unset on cluster {{.ClusterName}})</i>
		{{- else}}<i>(unknown — no cluster context)</i>
		{{- end}}
	</td></tr>
</table>

<section class="section">
	<h2>Members <span class="count">{{.Members | len}}</span></h2>
	{{if .Members}}
	<table class="grid">
		<thead>
			<tr>
				<th>Subchannel</th>
				<th>Name</th>
				<th>State</th>
				<th class="num">Calls started</th>
				<th class="num">Calls succeeded</th>
				<th class="num">Calls failed</th>
				<th>Socket(s)</th>
			</tr>
		</thead>
		<tbody>
			{{range .Members}}
			<tr>
				<td class="mono"><a href="{{link "subchannel" .SubchannelID}}"><b>{{.SubchannelID}}</b></a></td>
				<td>{{.Name}}</td>
				<td><span class="state {{.State | stateClass}}">{{.State}}</span></td>
				<td class="num">{{.CallsStarted}}</td>
				<td class="num">{{.CallsSucceeded}}</td>
				<td class="num">{{.CallsFailed}}</td>
				<td class="chips">
					{{range .SocketIDs}}
						<a class="chip" href="{{link "socket" .}}"><b>{{.}}</b></a>
					{{end}}
				</td>
			</tr>
			{{end}}
		</tbody>
	</table>
	{{else}}
		<span class="empty">No subchannels currently targeting this address.</span>
	{{end}}
</section>
`
