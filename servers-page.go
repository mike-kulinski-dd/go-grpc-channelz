package channelz

import (
	"context"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// writeServers writes HTML to w containing RPC servers stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeServers(w io.Writer) {
	data := h.getServers()
	if data == nil {
		writeUnavailable(w)
		return
	}
	if err := serversTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getServers() *channelzgrpc.GetServersResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	servers, err := client.GetServers(ctx, &channelzgrpc.GetServersRequest{})
	if err != nil {
		log.Errorf("Error querying GetServers %+v", err)
		return nil
	}
	return servers
}

const serversTemplateHTML = `
{{define "server-header"}}
    <tr>
        <th>Server</th>
		<th>CreationTimestamp</th>
        <th class="num">CallsStarted</th>
        <th class="num">CallsSucceeded</th>
        <th class="num">CallsFailed</th>
        <th>LastCallStartedTimestamp</th>
		<th>Sockets</th>
    </tr>
{{end}}

{{define "server-body"}}
    <tr>
        <td class="mono"><a href="{{link "server" .Ref.ServerId}}"><b>{{.Ref.ServerId}}</b> {{.Ref.Name}}</a></td>
        <td class="mono">{{with .Data.Trace}}{{.CreationTimestamp | timestamp}}{{end}}</td>
        <td class="num">{{.Data.CallsStarted}}</td>
        <td class="num">{{.Data.CallsSucceeded}}</td>
        <td class="num">{{.Data.CallsFailed}}</td>
        <td class="mono">{{.Data.LastCallStartedTimestamp | timestamp}}</td>
		<td class="chips">
			{{range .ListenSocket}}
				<a class="chip" href="{{link "socket" .SocketId}}"><b>{{.SocketId}}</b><span class="name">{{.Name}}</span></a>
			{{end}}
		</td>
	</tr>
	<tr class="subrow">
		<td colspan="7">
			<details>
				<summary>Events</summary>
				{{template "events" .Data.Trace}}
			</details>
		</td>
	</tr>
{{end}}

<section class="section">
	<h2>Servers: {{.Server | len}}</h2>
	<table class="grid">
		<thead>
			{{template "server-header"}}
		</thead>
		<tbody>
			{{range .Server}}
				{{template "server-body" .}}
			{{end}}
		</tbody>
	</table>
</section>
`
