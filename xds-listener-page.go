package channelz

import (
	"context"
	"fmt"
	"io"

	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// WriteXdsListenerPage writes the LDS view for an xDS-resolved channel.
func (h *grpcChannelzHandler) WriteXdsListenerPage(w io.Writer, channelID int64) {
	writeHeader(w, fmt.Sprintf("xDS Listener for channel %d", channelID))
	h.writeXdsListener(w, channelID)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeXdsListener(w io.Writer, channelID int64) {
	data := h.getXdsListener(channelID)
	if err := xdsListenerTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

type xdsListenerPageData struct {
	ChannelID       int64
	Target          string
	Authority       string
	ResourceName    string
	Listener        *listenerv3.Listener
	ListenerName    string
	RouteConfigName string
	InlineRoute     *routev3.RouteConfiguration
	Error           string
}

func (h *grpcChannelzHandler) getXdsListener(channelID int64) *xdsListenerPageData {
	d := &xdsListenerPageData{ChannelID: channelID}

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

	cfg, err := h.fetchClientStatus(context.Background())
	if err != nil {
		d.Error = fmt.Sprintf("CSDS FetchClientStatus failed: %v", err)
		return d
	}
	resources := newXdsResources(cfg)

	d.Listener = findListenerForChannel(resources, ch.GetChannel(), resource)
	if d.Listener == nil {
		d.Error = fmt.Sprintf("no LDS resource matches resource name %q", resource)
		return d
	}
	d.ListenerName = d.Listener.GetName()
	d.RouteConfigName = extractRouteConfigName(d.Listener)
	d.InlineRoute = extractInlineRouteConfig(d.Listener)
	return d
}

// findListenerForChannel picks the LDS resource for this channel. Prefers exact
// name match against the xds target's resource component; falls back to the
// only listener in the dump if there's just one.
func findListenerForChannel(r *xdsResources, _ *channelzgrpc.Channel, resourceName string) *listenerv3.Listener {
	if l, ok := r.listeners[resourceName]; ok {
		return l
	}
	for _, l := range r.listeners {
		if l.GetName() == resourceName {
			return l
		}
	}
	if len(r.listeners) == 1 {
		for _, l := range r.listeners {
			return l
		}
	}
	return nil
}

const xdsListenerTemplateHTML = `
{{if .Error}}
	<p><b>Error:</b> {{.Error}}</p>
{{end}}
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Channel</th><td><a href="{{link "channel" .ChannelID}}">{{.ChannelID}}</a></td></tr>
	<tr><th>Target</th><td>{{.Target}}</td></tr>
	<tr><th>Authority</th><td>{{if .Authority}}{{.Authority}}{{else}}<i>(default)</i>{{end}}</td></tr>
	<tr><th>Resource name</th><td>{{.ResourceName}}</td></tr>
{{if .Listener}}
	<tr><th>Listener name</th><td>{{.ListenerName}}</td></tr>
	<tr><th>Route config</th>
		<td>
		{{if .RouteConfigName}}
			<a href="{{link "xds" "route" .ChannelID .RouteConfigName}}">{{.RouteConfigName}}</a>
		{{else if .InlineRoute}}
			<i>inline:</i> {{.InlineRoute.Name}}
		{{else}}
			<i>(none)</i>
		{{end}}
		</td>
	</tr>
{{end}}
</table>
`
