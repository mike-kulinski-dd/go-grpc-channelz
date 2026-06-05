package channelz

import (
	"fmt"
	"html"
	"io"
	"strings"
	"text/template"
	"time"

	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	log "google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	common             *template.Template
	headerTemplate     = parseTemplate("header", headerTemplateHTML)
	channelsTemplate   = parseTemplate("channels", channelsTemplateHTML)
	subChannelTemplate = parseTemplate("subchannel", subChannelsTemplateHTML)
	channelTemplate    = parseTemplate("channel", channelTemplateHTML)
	serversTemplate    = parseTemplate("servers", serversTemplateHTML)
	serverTemplate     = parseTemplate("server", serverTemplateHTML)
	socketTemplate     = parseTemplate("socket", socketTemplateHTML)
	xdsVirtualHostTemplate = parseTemplate("xds-vhost", xdsVirtualHostTemplateHTML)
	xdsClusterTemplate     = parseTemplate("xds-cluster", xdsClusterTemplateHTML)
	footerTemplate     = parseTemplate("footer", footerTemplateHTML)
)

func parseTemplate(name, html string) *template.Template {
	if common == nil {
		common = template.Must(template.New(name).Funcs(getFuncs()).Parse(html))
		return common
	}
	common = template.Must(common.New(name).Funcs(getFuncs()).Parse(html))
	return common
}

func getFuncs() template.FuncMap {
	return template.FuncMap{
		"timestamp":    formatTimestamp,
		"link":         createHyperlink,
		"isXds":        isXdsTarget,
		"matchSummary": routeMatchSummary,
	}
}

func isXdsTarget(target string) bool {
	_, _, ok := parseXdsTarget(target)
	return ok
}

func routeMatchSummary(m *routev3.RouteMatch) string {
	if m == nil {
		return ""
	}
	parts := []string{}
	switch ps := m.GetPathSpecifier().(type) {
	case *routev3.RouteMatch_Prefix:
		parts = append(parts, fmt.Sprintf("prefix=%q", ps.Prefix))
	case *routev3.RouteMatch_Path:
		parts = append(parts, fmt.Sprintf("path=%q", ps.Path))
	case *routev3.RouteMatch_SafeRegex:
		parts = append(parts, fmt.Sprintf("regex=%q", ps.SafeRegex.GetRegex()))
	case *routev3.RouteMatch_ConnectMatcher_:
		parts = append(parts, "connect")
	case *routev3.RouteMatch_PathSeparatedPrefix:
		parts = append(parts, fmt.Sprintf("path_separated_prefix=%q", ps.PathSeparatedPrefix))
	}
	for _, h := range m.GetHeaders() {
		parts = append(parts, fmt.Sprintf("header[%s]", h.GetName()))
	}
	return strings.Join(parts, "\n")
}

func formatTimestamp(ts *timestamppb.Timestamp) string {
	return ts.AsTime().Format(time.RFC3339)
}

func writeHeader(w io.Writer, title string) {
	if err := headerTemplate.Execute(w, headerData{Title: title}); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

// writeRawDump renders a collapsible <details> block containing the YAML dump
// of one or more proto messages — the underlying config for the page.
func writeRawDump(w io.Writer, sections ...rawDumpSection) {
	if len(sections) == 0 {
		return
	}
	fmt.Fprintln(w, `<details style="margin-top:1em" open><summary><b>Raw config dump</b></summary>`)
	for _, s := range sections {
		if s.Title != "" {
			fmt.Fprintf(w, "<h4>%s</h4>\n", html.EscapeString(s.Title))
		}
		if s.Msg == nil || !s.Msg.ProtoReflect().IsValid() {
			fmt.Fprintln(w, "<pre><i>(not available)</i></pre>")
			continue
		}
		fmt.Fprintf(w, "<pre>%s</pre>\n", html.EscapeString(protoToYAML(s.Msg)))
	}
	fmt.Fprintln(w, `</details>`)
}

type rawDumpSection struct {
	Title string
	Msg   proto.Message
}

func writeFooter(w io.Writer) {
	if err := footerTemplate.Execute(w, nil); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

// headerData contains data for the header template.
type headerData struct {
	Title string
}

var (
	headerTemplateHTML = `
<!DOCTYPE html>
<html lang="en"><head>
    <meta charset="utf-8">
    <title>{{.Title}}</title>
    <link rel="stylesheet" href="https://fonts.googleapis.com/icon?family=Material+Icons">
    <link rel="stylesheet" href="https://code.getmdl.io/1.3.0/material.indigo-pink.min.css">
	<style>
		body {padding: 1em}
		table {
			background-color: #fff5ee;
		}
		table.section-header {
			background-color: #eeeeff;
			font-size: x-large;
		}
		table.vertical th {
			text-align: right;
			padding-right: 1em;
		}
		tr.header {
			background-color: #eee5de;
		}
		td {
			vertical-align: top;
		}
		footer {
			padding-top: 1em;
		}
	</style>
</head>
<body>
<h1>{{.Title}}</h1>
`

	footerTemplateHTML = `
<footer>
	<a href="https://github.com/grpc/proposal/blob/master/A14-channelz.md" target="spec">Channelz Spec</a>
</footer>
</body>
</html>
`
)
