package channelz

import (
	"context"
	"fmt"
	"io"

	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	log "google.golang.org/grpc/grpclog"
)

// WriteXdsRoutePage writes the RDS view for a route configuration referenced by an xDS channel.
func (h *grpcChannelzHandler) WriteXdsRoutePage(w io.Writer, channelID int64, routeName string) {
	writeHeader(w, fmt.Sprintf("xDS Route %s (channel %d)", routeName, channelID))
	h.writeXdsRoute(w, channelID, routeName)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeXdsRoute(w io.Writer, channelID int64, routeName string) {
	d := h.getXdsRoute(channelID, routeName)
	if err := xdsRouteTemplate.Execute(w, d); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

type xdsRoutePageData struct {
	ChannelID   int64
	RouteName   string
	RouteConfig *routev3.RouteConfiguration
	Error       string
}

func (h *grpcChannelzHandler) getXdsRoute(channelID int64, routeName string) *xdsRoutePageData {
	d := &xdsRoutePageData{ChannelID: channelID, RouteName: routeName}

	cfg, err := h.fetchClientStatus(context.Background())
	if err != nil {
		d.Error = fmt.Sprintf("CSDS FetchClientStatus failed: %v", err)
		return d
	}
	resources := newXdsResources(cfg)

	if rc, ok := resources.routes[routeName]; ok {
		d.RouteConfig = rc
		return d
	}
	// Fallback: inline route on the channel's listener.
	ch := h.getChannel(channelID)
	if ch != nil && ch.GetChannel() != nil {
		_, resource, _ := parseXdsTarget(ch.GetChannel().GetData().GetTarget())
		l := findListenerForChannel(resources, ch.GetChannel(), resource)
		if l != nil {
			if inline := extractInlineRouteConfig(l); inline != nil && inline.GetName() == routeName {
				d.RouteConfig = inline
				return d
			}
		}
	}
	d.Error = fmt.Sprintf("no RDS resource named %q in CSDS dump", routeName)
	return d
}

// routeClusterLink returns the target cluster name for a Route, or "" if the
// route uses weighted clusters / cluster header / specifier plugin.
func routeClusterLink(r *routev3.Route) string {
	if r == nil {
		return ""
	}
	return r.GetRoute().GetCluster()
}

const xdsRouteTemplateHTML = `
{{if .Error}}
	<p><b>Error:</b> {{.Error}}</p>
{{end}}
<p><a href="{{link "xds" "listener" .ChannelID}}">&laquo; back to listener</a></p>
{{if .RouteConfig}}
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
	<tr><th>Name</th><td>{{.RouteConfig.Name}}</td></tr>
</table>
{{range .RouteConfig.VirtualHosts}}
<h3>VirtualHost: {{.Name}}</h3>
<table frame=box cellspacing=0 cellpadding=2>
	<tr><th>Domains</th><td>{{range $i, $d := .Domains}}{{if $i}}, {{end}}{{$d}}{{end}}</td></tr>
</table>
<table frame=box cellspacing=0 cellpadding=2>
	<tr class="header">
		<th>Match</th>
		<th>Cluster</th>
		<th>Weighted clusters</th>
	</tr>
	{{range .Routes}}
	<tr>
		<td><pre>{{.Match | matchSummary}}</pre></td>
		<td>
			{{with .Route.GetCluster}}
				<a href="{{link "xds" "cluster" $.ChannelID .}}">{{.}}</a>
			{{end}}
		</td>
		<td>
			{{with .Route.GetWeightedClusters}}
				{{range .Clusters}}
					<a href="{{link "xds" "cluster" $.ChannelID .Name}}">{{.Name}}</a> ({{.Weight.Value}})<br/>
				{{end}}
			{{end}}
		</td>
	</tr>
	{{end}}
</table>
{{end}}
{{end}}
`
