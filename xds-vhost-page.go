package channelz

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	log "google.golang.org/grpc/grpclog"
)

// WriteXdsVirtualHostsPage writes the summary of every VHDS resource an
// xDS-resolved channel's client holds: the dial-time vhost and any call-time
// ones, each linking to its own page.
func (h *grpcChannelzHandler) WriteXdsVirtualHostsPage(w io.Writer, channelID int64) {
	writeHeader(w, fmt.Sprintf("xDS VirtualHosts for channel %d", channelID))
	data := h.getXdsVirtualHost(channelID)
	if err := xdsVirtualHostsTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
	writeRawDump(w, h.reflectionResolver(), rawDumpSection{Title: "Node", Msg: data.Node})
	writeFooter(w)
}

// WriteXdsVirtualHostPage writes one VHDS resource's routes for a channel.
func (h *grpcChannelzHandler) WriteXdsVirtualHostPage(w io.Writer, channelID int64, resourceName string) {
	writeHeader(w, fmt.Sprintf("xDS VirtualHost %s", resourceName))
	data := h.getXdsVirtualHost(channelID)
	page := xdsVirtualHostDetailData{ChannelID: channelID, Target: data.Target, Error: data.Error}
	for i := range data.VirtualHosts {
		if data.VirtualHosts[i].ResourceName == resourceName {
			page.VirtualHost = &data.VirtualHosts[i]
		}
	}
	if page.VirtualHost == nil && page.Error == "" {
		page.Error = fmt.Sprintf("no VHDS resource %q in the CSDS dump", resourceName)
	}
	if err := xdsVirtualHostTemplate.Execute(w, page); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
	if page.VirtualHost != nil {
		writeRawDump(w, h.reflectionResolver(), rawDumpSection{Title: "VirtualHost", Msg: page.VirtualHost.VirtualHost})
	}
	writeFooter(w)
}

type xdsVirtualHostDetailData struct {
	ChannelID   int64
	Target      string
	VirtualHost *xdsVirtualHostSection
	Error       string
}

type xdsVirtualHostPageData struct {
	ChannelID        int64
	Target           string
	ResourceName     string
	VHDSResourceName string
	// VirtualHosts lists every VHDS resource in the CSDS dump, the channel's
	// dial-time one first. dashmesh discovers a grpc.CallAuthority override's
	// vhost on demand on the same channel, so the others are call-time
	// authorities, unless another xds channel dials them.
	VirtualHosts []xdsVirtualHostSection
	Node         *corev3.Node
	Error        string
}

// xdsVirtualHostSection is one VHDS resource and its routes.
type xdsVirtualHostSection struct {
	ResourceName string
	// Role is "dial-time", "call-time", or "dial-time of channel N".
	Role         string
	OtherChannel int64
	// Authority is the request authority this vhost serves, from its
	// "sidecar/http2/<authority>" resource name.
	Authority string
	// Domains is set only when it differs from just Authority.
	Domains     []string
	VirtualHost *routev3.VirtualHost
	Routes      []xdsRouteRow
}

// xdsRouteRow is a Route pre-rendered for the template: the match condition is
// dumped as YAML server-side (via renderProtoYAMLHTML) rather than distilled
// into a lossy one-line summary.
type xdsRouteRow struct {
	MatchYAML        string
	Cluster          string
	WeightedClusters []weightedClusterRow
}

type weightedClusterRow struct {
	Name   string
	Weight uint32
}

func (h *grpcChannelzHandler) getXdsVirtualHost(channelID int64) *xdsVirtualHostPageData {
	d := &xdsVirtualHostPageData{ChannelID: channelID}

	ch := h.getChannel(channelID)
	if ch == nil || ch.GetChannel() == nil {
		d.Error = "channel not found"
		return d
	}
	d.Target = ch.GetChannel().GetData().GetTarget()

	_, resource, ok := parseXdsTarget(d.Target)
	if !ok {
		d.Error = fmt.Sprintf("channel target %q is not an xds:// target", d.Target)
		return d
	}
	d.ResourceName = resource
	d.VHDSResourceName = vhdsResourceName(resource)

	cfg, err := h.fetchClientStatus(context.Background())
	if err != nil {
		d.Error = fmt.Sprintf("CSDS FetchClientStatus failed: %v", err)
		return d
	}
	d.Node = cfg.GetNode()
	resources := newXdsResources(cfg)

	dialTime := findVirtualHostForChannel(resources, resource)
	if dialTime == "" {
		d.Error = fmt.Sprintf("no VHDS resource matches %q (looked up as %q). CSDS dump contains %d VHDS resource(s): %v", resource, d.VHDSResourceName, len(resources.virtualHosts), sortedKeys(resources.virtualHosts))
	}

	// Other xds channels' dial-time vhosts share this CSDS dump.
	otherChannels := map[string]int64{}
	for _, c := range h.getTopChannels(0).GetChannel() {
		if c.GetRef().GetChannelId() == channelID {
			continue
		}
		if _, res, ok := parseXdsTarget(c.GetData().GetTarget()); ok {
			otherChannels[vhdsResourceName(res)] = c.GetRef().GetChannelId()
		}
	}

	resolver := h.reflectionResolver()
	names := sortedKeys(resources.virtualHosts)
	if dialTime != "" {
		names = append([]string{dialTime}, slices.DeleteFunc(names, func(n string) bool { return n == dialTime })...)
	}
	for _, name := range names {
		v := resources.virtualHosts[name]
		sec := xdsVirtualHostSection{ResourceName: name, Role: "call-time", Authority: strings.TrimPrefix(name, "sidecar/http2/"), VirtualHost: v}
		if d := v.GetDomains(); !slices.Equal(d, []string{sec.Authority}) {
			sec.Domains = d
		}
		switch id, ok := otherChannels[name]; {
		case name == dialTime:
			sec.Role = "dial-time"
		case ok:
			sec.Role, sec.OtherChannel = "dial-time of channel", id
		}
		for _, route := range v.GetRoutes() {
			row := xdsRouteRow{
				MatchYAML: renderProtoYAMLHTML(route.GetMatch(), resolver),
				Cluster:   route.GetRoute().GetCluster(),
			}
			for _, wc := range route.GetRoute().GetWeightedClusters().GetClusters() {
				row.WeightedClusters = append(row.WeightedClusters, weightedClusterRow{
					Name:   wc.GetName(),
					Weight: wc.GetWeight().GetValue(),
				})
			}
			sec.Routes = append(sec.Routes, row)
		}
		d.VirtualHosts = append(d.VirtualHosts, sec)
	}
	return d
}

// vhdsResourceName mirrors the dashmesh grpcxds resolver, which dials VHDS for
// "sidecar/http2/<target-endpoint>" rather than the raw xds:// resource.
func vhdsResourceName(targetResource string) string {
	return "sidecar/http2/" + targetResource
}

// findVirtualHostForChannel returns the VHDS resource name for this channel's
// dial-time vhost: the dashmesh-prefixed name ("sidecar/http2/<resource>"),
// then the raw resource, then a name-field match. "" if none matches; with
// call-time vhosts in the dump, a lone resource isn't necessarily this one.
func findVirtualHostForChannel(r *xdsResources, resourceName string) string {
	for _, candidate := range []string{vhdsResourceName(resourceName), resourceName} {
		if _, ok := r.virtualHosts[candidate]; ok {
			return candidate
		}
	}
	for n, v := range r.virtualHosts {
		if v.GetName() == resourceName {
			return n
		}
	}
	return ""
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

const xdsVirtualHostsTemplateHTML = `
{{if .Error}}
	<p><b>Error:</b> {{.Error}}</p>
{{end}}
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Channel</th><td><a href="{{link "channel" .ChannelID}}">{{.ChannelID}}</a></td></tr>
	<tr><th>Target</th><td>{{.Target}}</td></tr>
	<tr><th>Dial-time VHDS resource</th><td>{{.VHDSResourceName}}</td></tr>
</table>

<h3>Virtual hosts</h3>
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>VHDS resource</th>
		<th>Authority</th>
		<th>Role</th>
		<th>Routes</th>
	</tr>
	{{range .VirtualHosts}}
	<tr>
		<td><a href="{{link "xds" "vhost" $.ChannelID .ResourceName}}">{{.ResourceName}}</a></td>
		<td>{{.Authority}}{{with .Domains}}<br/><i>domains:</i> {{range .}}{{.}} {{end}}{{end}}</td>
		<td>{{.Role}}{{if .OtherChannel}} <a href="{{link "channel" .OtherChannel}}">{{.OtherChannel}}</a>{{end}}</td>
		<td>{{len .Routes}}</td>
	</tr>
	{{end}}
</table>
`

const xdsVirtualHostTemplateHTML = `
{{if .Error}}
	<p><b>Error:</b> {{.Error}}</p>
{{end}}
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Channel</th><td><a href="{{link "channel" .ChannelID}}">{{.ChannelID}}</a> (<a href="{{link "xds" "vhost" .ChannelID}}">all virtual hosts</a>)</td></tr>
	<tr><th>Target</th><td>{{.Target}}</td></tr>
{{with .VirtualHost}}
	<tr><th>VHDS resource</th><td>{{.ResourceName}}</td></tr>
	<tr><th>Authority</th><td>{{.Authority}}</td></tr>
	<tr><th>Role</th><td>{{.Role}}{{if .OtherChannel}} <a href="{{link "channel" .OtherChannel}}">{{.OtherChannel}}</a>{{end}}</td></tr>
{{with .Domains}}
	<tr><th>Domains</th><td>{{range .}}{{.}}<br/>{{end}}</td></tr>
{{end}}
</table>

<h3>Routes</h3>
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>Match</th>
		<th>Cluster</th>
		<th>Weighted clusters</th>
	</tr>
	{{range .Routes}}
	<tr>
		<td><pre>{{.MatchYAML}}</pre></td>
		<td>
			{{with .Cluster}}
				<a href="{{link "xds" "cluster" $.ChannelID .}}">{{.}}</a>
			{{end}}
		</td>
		<td>
			{{range .WeightedClusters}}
				<a href="{{link "xds" "cluster" $.ChannelID .Name}}">{{.Name}}</a> ({{.Weight}})<br/>
			{{end}}
		</td>
	</tr>
	{{end}}
</table>
{{else}}
</table>
{{end}}
`
