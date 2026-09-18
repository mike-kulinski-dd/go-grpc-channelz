package channelz

import (
	"fmt"
	"html"
	"io"
	"net"
	"text/template"
	"time"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
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
	subchannelPoolsTemplate = parseTemplate("subchannel-pools", subchannelPoolsTemplateHTML)
	subchannelPoolTemplate  = parseTemplate("subchannel-pool", subchannelPoolTemplateHTML)
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
		"timestamp": formatTimestamp,
		"link":      createHyperlink,
		"isXds":     isXdsTarget,
		"address":   formatAddress,
	}
}

// formatAddress renders a channelz Address proto as a human-readable string,
// e.g. "127.0.0.1:16001" for a TCP/IP address, rather than the raw byte dump
// its default String() produces.
func formatAddress(addr *channelzgrpc.Address) string {
	if addr == nil {
		return ""
	}
	switch {
	case addr.GetTcpipAddress() != nil:
		tcp := addr.GetTcpipAddress()
		return net.JoinHostPort(net.IP(tcp.GetIpAddress()).String(), fmt.Sprintf("%d", tcp.GetPort()))
	case addr.GetUdsAddress() != nil:
		return addr.GetUdsAddress().GetFilename()
	case addr.GetOtherAddress() != nil:
		return addr.GetOtherAddress().GetName()
	default:
		return ""
	}
}

func isXdsTarget(target string) bool {
	_, _, ok := parseXdsTarget(target)
	return ok
}

// renderProtoYAMLHTML renders a proto message as HTML-escaped YAML for direct
// use inside a <pre> block in a text/template (which does not auto-escape).
// resolver expands any nested google.protobuf.Any fields; pass nil to fall
// back to protoregistry.GlobalTypes.
func renderProtoYAMLHTML(m proto.Message, resolver protoResolver) string {
	if m == nil || !m.ProtoReflect().IsValid() {
		return ""
	}
	return html.EscapeString(protoToYAML(m, resolver))
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
// resolver expands any nested google.protobuf.Any fields; pass nil to fall
// back to protoregistry.GlobalTypes.
func writeRawDump(w io.Writer, resolver protoResolver, sections ...rawDumpSection) {
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
		fmt.Fprintf(w, "<pre>%s</pre>\n", html.EscapeString(protoToYAML(s.Msg, resolver)))
	}
	fmt.Fprintln(w, `</details>`)
}

type rawDumpSection struct {
	Title string
	Msg   proto.Message
}

// writeUnavailable renders a friendly message in place of a page whose data
// couldn't be fetched (e.g. the target server is unreachable), instead of
// executing a template against a nil response and producing a confusing
// "nil pointer evaluating ..." template error.
func writeUnavailable(w io.Writer) {
	fmt.Fprintln(w, `<p><i>Unable to reach the target server. Check its connection and reload this page.</i></p>`)
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
