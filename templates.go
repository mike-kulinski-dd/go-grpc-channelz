package channelz

import (
	"fmt"
	"html"
	"io"
	"net"
	"strings"
	"text/template"
	"time"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	common             *template.Template
	layoutTemplate     = parseTemplate("layout", layoutTemplateHTML)
	headerTemplate     = parseTemplate("header", headerTemplateHTML)
	channelsTemplate   = parseTemplate("channels", channelsTemplateHTML)
	subChannelTemplate = parseTemplate("subchannel", subChannelsTemplateHTML)
	channelTemplate    = parseTemplate("channel", channelTemplateHTML)
	serversTemplate    = parseTemplate("servers", serversTemplateHTML)
	serverTemplate     = parseTemplate("server", serverTemplateHTML)
	socketTemplate     = parseTemplate("socket", socketTemplateHTML)
	xdsVirtualHostTemplate = parseTemplate("xds-vhost", xdsVirtualHostTemplateHTML)
	xdsClusterTemplate     = parseTemplate("xds-cluster", xdsClusterTemplateHTML)
	subchannelPoolTemplate = parseTemplate("subchannel-pool", subchannelPoolTemplateHTML)
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
		"timestamp":  formatTimestamp,
		"link":       createHyperlink,
		"isXds":      isXdsTarget,
		"address":    formatAddress,
		"stateClass": stateClass,
	}
}

// stateClass turns a channelz connectivity state into the CSS class for its
// `.state--*` badge. Templates pass either a *ChannelConnectivityState
// (rendered by its generated Stringer as "state:READY") or an already-
// stringified state (e.g. poolMemberRow.State); accepting `any` lets one
// func cover both. Unknown/empty states fall back to a neutral badge rather
// than an empty class.
func stateClass(state any) string {
	var s string
	switch v := state.(type) {
	case nil:
	case string:
		s = v
	case fmt.Stringer:
		s = v.String()
	default:
		s = fmt.Sprint(v)
	}
	if idx := strings.LastIndexByte(s, ':'); idx >= 0 {
		s = s[idx+1:]
	}
	switch strings.ToUpper(s) {
	case "READY", "CONNECTING", "IDLE", "TRANSIENT_FAILURE", "SHUTDOWN":
		return "state--" + strings.ToLower(s)
	default:
		return "state--unknown"
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
	fmt.Fprintln(w, `<details class="rawdump" open><summary>Raw config dump</summary>`)
	for _, s := range sections {
		if s.Title != "" {
			fmt.Fprintf(w, "<h4>%s</h4>\n", html.EscapeString(s.Title))
		}
		if s.Msg == nil || !s.Msg.ProtoReflect().IsValid() {
			fmt.Fprintln(w, `<div class="panel"><pre><i>(not available)</i></pre></div>`)
			continue
		}
		fmt.Fprintf(w, "<div class=\"panel\"><pre>%s</pre></div>\n", html.EscapeString(protoToYAML(s.Msg, resolver)))
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
	fmt.Fprintln(w, `<div class="notice notice--error">Unable to reach the target server. Check its connection and reload this page.</div>`)
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
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>{{.Title}}</title>
	<style>
		:root {
			--surface: #ffffff;
			--surface-raised: #f7f8fa;
			--surface-inset: #f2f3f5;
			--border: #dde1e6;
			--text: #1c2126;
			--text-muted: #5b6470;
			--accent: #2f6feb;
			--accent-muted: #eaf1ff;
			--zebra: #f7f8fa;
			--state-ready: #1a7f4f;
			--state-ready-bg: #e6f6ee;
			--state-connecting: #9a6b00;
			--state-connecting-bg: #fdf3d8;
			--state-idle: #5b6470;
			--state-idle-bg: #eef0f2;
			--state-transient_failure: #b3261e;
			--state-transient_failure-bg: #fbe9e7;
			--state-shutdown: #5b6470;
			--state-shutdown-bg: #eef0f2;
			--state-unknown: #5b6470;
			--state-unknown-bg: #eef0f2;
			--topbar-height: 3rem;
		}
		* { box-sizing: border-box; }
		body {
			margin: 0;
			padding: 0 1.25rem 2rem;
			background: var(--surface);
			color: var(--text);
			font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
		}
		.mono, table.grid td.mono, table.detail td.mono, .chip, pre, code {
			font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
		}
		a { color: var(--accent); text-decoration: none; }
		a:hover { text-decoration: underline; }
		h1, h2, h3, h4 { font-weight: 600; margin: 1.5rem 0 .5rem; }
		.topbar {
			position: sticky;
			top: 0;
			z-index: 20;
			display: flex;
			align-items: center;
			gap: .75rem;
			height: var(--topbar-height);
			margin: 0 -1.25rem 1rem;
			padding: 0 1.25rem;
			background: var(--surface-raised);
			border-bottom: 1px solid var(--border);
		}
		.topbar .home {
			font-weight: 700;
			color: var(--text);
			letter-spacing: .01em;
		}
		.topbar .title {
			color: var(--text-muted);
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
		}
		.notice {
			padding: .6rem .9rem;
			border-radius: .4rem;
			background: var(--surface-inset);
			color: var(--text-muted);
			margin: .5rem 0 1rem;
		}
		.notice--error {
			background: var(--state-transient_failure-bg);
			color: var(--state-transient_failure);
		}
		.section { margin: 1.5rem 0; }
		.section > h2 {
			display: flex;
			align-items: baseline;
			gap: .5rem;
		}
		.section > h2 .count {
			font-size: .75rem;
			font-weight: 600;
			color: var(--text-muted);
			background: var(--surface-inset);
			border-radius: 1rem;
			padding: .1rem .55rem;
		}
		table { border-collapse: separate; border-spacing: 0; width: 100%; }
		table.detail {
			max-width: 60rem;
		}
		table.detail th, table.detail td {
			text-align: left;
			padding: .4rem .75rem;
			border-bottom: 1px solid var(--border);
			vertical-align: top;
		}
		table.detail th {
			width: 14rem;
			min-width: 10rem;
			color: var(--text-muted);
			font-weight: 500;
			border-right: 1px solid var(--border);
			white-space: nowrap;
		}
		table.grid thead th {
			position: sticky;
			top: var(--topbar-height);
			z-index: 10;
			background: var(--surface-raised);
			border-bottom: 2px solid var(--border);
			text-align: left;
			padding: .5rem .75rem;
			font-weight: 600;
			white-space: nowrap;
		}
		table.grid th + th, table.grid td + td {
			border-left: 1px solid var(--border);
		}
		table.grid td {
			padding: .45rem .75rem;
			vertical-align: top;
			border-bottom: 1px solid var(--border);
		}
		table.grid tbody tr:nth-child(even) { background: var(--zebra); }
		table.grid tbody tr:hover { background: var(--accent-muted); }
		table.grid tr.subrow td { background: var(--surface-inset); }
		td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
		.panel {
			max-height: 22rem;
			overflow: auto;
			border: 1px solid var(--border);
			border-radius: .4rem;
			background: var(--surface-inset);
		}
		.panel--chips { max-height: 16rem; padding: .6rem; }
		.panel--options { padding: .5rem .9rem; }
		.chips { display: flex; flex-wrap: wrap; gap: .4rem; }
		.chip {
			display: inline-flex;
			align-items: baseline;
			gap: .35rem;
			padding: .2rem .55rem;
			border: 1px solid var(--border);
			border-radius: 1rem;
			background: var(--surface);
			font-size: .85rem;
			white-space: nowrap;
		}
		a.chip:hover { border-color: var(--accent); background: var(--accent-muted); text-decoration: none; }
		.chip b { font-weight: 700; }
		.chip .name { color: var(--text-muted); }
		.empty { color: var(--text-muted); font-style: italic; }
		.events { padding: .6rem .9rem; }
		.events pre {
			margin: 0;
			white-space: pre-wrap;
			word-break: break-word;
			line-height: 1.5;
		}
		.state {
			display: inline-block;
			padding: .1rem .5rem;
			border-radius: .3rem;
			font-size: .8rem;
			font-weight: 600;
		}
		.state--ready { color: var(--state-ready); background: var(--state-ready-bg); }
		.state--connecting { color: var(--state-connecting); background: var(--state-connecting-bg); }
		.state--idle { color: var(--state-idle); background: var(--state-idle-bg); }
		.state--transient_failure { color: var(--state-transient_failure); background: var(--state-transient_failure-bg); }
		.state--shutdown { color: var(--state-shutdown); background: var(--state-shutdown-bg); }
		.state--unknown { color: var(--state-unknown); background: var(--state-unknown-bg); }
		.pager { margin: .75rem 0; }
		details.rawdump { margin-top: 1.5rem; }
		details.rawdump > summary {
			cursor: pointer;
			font-weight: 600;
			padding: .3rem 0;
		}
		details.rawdump h4 { margin: 1rem 0 .35rem; }
		footer {
			margin-top: 2rem;
			padding-top: 1rem;
			border-top: 1px solid var(--border);
			color: var(--text-muted);
		}
	</style>
</head>
<body>
<div class="topbar">
	<a class="home" href="{{link}}/">channelz</a>
	<span class="title">{{.Title}}</span>
</div>
`

	footerTemplateHTML = `
<footer>
	<a href="https://github.com/grpc/proposal/blob/master/A14-channelz.md" target="spec">Channelz Spec</a>
</footer>
</body>
</html>
`

	// layoutTemplateHTML holds {{define}} blocks shared by every page template.
	// It's registered first so its defines land in the root of the shared
	// "common" template tree (see parseTemplate) and stay visible everywhere.
	layoutTemplateHTML = `
{{define "chips-channel"}}
	{{if .}}
	<div class="panel panel--chips"><div class="chips">
		{{range .}}
			<a class="chip" href="{{link "channel" .ChannelId}}"><b>{{.ChannelId}}</b><span class="name">{{.Name}}</span></a>
		{{end}}
	</div></div>
	{{else}}
		<span class="empty">none</span>
	{{end}}
{{end}}

{{define "chips-subchannel"}}
	{{if .}}
	<div class="panel panel--chips"><div class="chips">
		{{range .}}
			<a class="chip" href="{{link "subchannel" .SubchannelId}}"><b>{{.SubchannelId}}</b><span class="name">{{.Name}}</span></a>
		{{end}}
	</div></div>
	{{else}}
		<span class="empty">none</span>
	{{end}}
{{end}}

{{define "chips-socket"}}
	{{if .}}
	<div class="panel panel--chips"><div class="chips">
		{{range .}}
			<a class="chip" href="{{link "socket" .SocketId}}"><b>{{.SocketId}}</b><span class="name">{{.Name}}</span></a>
		{{end}}
	</div></div>
	{{else}}
		<span class="empty">none</span>
	{{end}}
{{end}}

{{define "events"}}
	{{if and . .Events}}
	<div class="panel events"><pre>
	{{- range .Events}}
{{.Severity}} [{{.Timestamp | timestamp}}]: {{.Description}}
	{{- end -}}
	</pre></div>
	{{else}}
		<span class="empty">no events</span>
	{{end}}
{{end}}
`
)
