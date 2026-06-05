package channelz

import (
	"context"
	"fmt"
	"io"
	"sort"

	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	log "google.golang.org/grpc/grpclog"
)

// WriteXdsVirtualHostPage writes the VHDS view for an xDS-resolved channel.
func (h *grpcChannelzHandler) WriteXdsVirtualHostPage(w io.Writer, channelID int64) {
	writeHeader(w, fmt.Sprintf("xDS VirtualHost for channel %d", channelID))
	h.writeXdsVirtualHost(w, channelID)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeXdsVirtualHost(w io.Writer, channelID int64) {
	data := h.getXdsVirtualHost(channelID)
	if err := xdsVirtualHostTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

type xdsVirtualHostPageData struct {
	ChannelID        int64
	Target           string
	Authority        string
	ResourceName     string
	VHDSResourceName string
	VirtualHost      *routev3.VirtualHost
	Error            string
}

func (h *grpcChannelzHandler) getXdsVirtualHost(channelID int64) *xdsVirtualHostPageData {
	d := &xdsVirtualHostPageData{ChannelID: channelID}

	ch := h.getChannel(channelID)
	if ch == nil || ch.GetChannel() == nil {
		d.Error = "channel not found"
		return d
	}
	d.Target = ch.GetChannel().GetData().GetTarget()

	authority, resource, ok := parseXdsTarget(d.Target)
	if !ok {
		d.Error = fmt.Sprintf("channel target %q is not an xds:// target", d.Target)
		return d
	}
	d.Authority = authority
	d.ResourceName = resource
	d.VHDSResourceName = vhdsResourceName(resource)

	cfg, err := h.fetchClientStatus(context.Background())
	if err != nil {
		d.Error = fmt.Sprintf("CSDS FetchClientStatus failed: %v", err)
		return d
	}
	resources := newXdsResources(cfg)

	d.VirtualHost = findVirtualHostForChannel(resources, resource)
	if d.VirtualHost == nil {
		names := make([]string, 0, len(resources.virtualHosts))
		for n := range resources.virtualHosts {
			names = append(names, n)
		}
		sort.Strings(names)
		d.Error = fmt.Sprintf("no VHDS resource matches %q (looked up as %q). CSDS dump contains %d VHDS resource(s): %v", resource, d.VHDSResourceName, len(names), names)
		return d
	}
	return d
}

// vhdsResourceName mirrors the dashmesh grpcxds resolver, which dials VHDS for
// "sidecar/http2/<target-endpoint>" rather than the raw xds:// resource.
func vhdsResourceName(targetResource string) string {
	return "sidecar/http2/" + targetResource
}

// findVirtualHostForChannel picks the VHDS resource for this channel. Tries
// the dashmesh-prefixed name first ("sidecar/http2/<resource>"), then the raw
// resource, then a name-field match, and finally falls back to the only
// virtual host in the dump if there's just one.
func findVirtualHostForChannel(r *xdsResources, resourceName string) *routev3.VirtualHost {
	for _, candidate := range []string{vhdsResourceName(resourceName), resourceName} {
		if v, ok := r.virtualHosts[candidate]; ok {
			return v
		}
	}
	for _, v := range r.virtualHosts {
		if v.GetName() == resourceName {
			return v
		}
	}
	if len(r.virtualHosts) == 1 {
		for _, v := range r.virtualHosts {
			return v
		}
	}
	return nil
}

const xdsVirtualHostTemplateHTML = `
{{if .Error}}
	<p><b>Error:</b> {{.Error}}</p>
{{end}}
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Channel</th><td><a href="{{link "channel" .ChannelID}}">{{.ChannelID}}</a></td></tr>
	<tr><th>Target</th><td>{{.Target}}</td></tr>
	<tr><th>Authority</th><td>{{if .Authority}}{{.Authority}}{{else}}<i>(default)</i>{{end}}</td></tr>
	<tr><th>Resource name</th><td>{{.ResourceName}}</td></tr>
	<tr><th>VHDS resource name</th><td>{{.VHDSResourceName}}</td></tr>
{{if .VirtualHost}}
	<tr><th>VirtualHost name</th><td>{{.VirtualHost.Name}}</td></tr>
	<tr><th>Domains</th><td>{{range $i, $d := .VirtualHost.Domains}}{{if $i}}, {{end}}{{$d}}{{end}}</td></tr>
</table>

<h3>Routes</h3>
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>Match</th>
		<th>Cluster</th>
		<th>Weighted clusters</th>
	</tr>
	{{range .VirtualHost.Routes}}
	<tr>
		<td><pre>{{.Match | matchSummary}}</pre></td>
		<td>
			{{with .GetRoute.GetCluster}}
				<a href="{{link "xds" "cluster" $.ChannelID .}}">{{.}}</a>
			{{end}}
		</td>
		<td>
			{{with .GetRoute.GetWeightedClusters}}
				{{range .Clusters}}
					<a href="{{link "xds" "cluster" $.ChannelID .Name}}">{{.Name}}</a> ({{.Weight.Value}})<br/>
				{{end}}
			{{end}}
		</td>
	</tr>
	{{end}}
</table>
{{else}}
</table>
{{end}}
`
