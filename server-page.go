package channelz

import (
	"context"
	"fmt"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

func (h *grpcChannelzHandler) WriteServerPage(w io.Writer, server int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ server %d", server))
	h.writeServer(w, server)
	writeFooter(w)
}

// writeServer writes HTML to w containing RPC single server stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeServer(w io.Writer, server int64) {
	data := h.getServer(server)
	if data == nil {
		writeUnavailable(w)
		return
	}
	if err := serverTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getServer(serverID int64) *channelzgrpc.GetServerResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	server, err := client.GetServer(ctx, &channelzgrpc.GetServerRequest{ServerId: serverID})
	if err != nil {
		log.Errorf("Error querying GetServer %+v", err)
		return nil
	}
	return server
}

const serverTemplateHTML = `
<table class="detail">
    <tr><th>ServerId</th><td class="mono">{{.Server.Ref.ServerId}}</td></tr>
    <tr><th>Server Name</th><td class="mono">{{.Server.Ref.Name}}</td></tr>
	<tr><th>CreationTimestamp</th><td class="mono">{{with .Server.Data.Trace}}{{.CreationTimestamp | timestamp}}{{end}}</td></tr>
	<tr><th>CallsStarted</th><td class="num">{{.Server.Data.CallsStarted}}</td></tr>
	<tr><th>CallsSucceeded</th><td class="num">{{.Server.Data.CallsSucceeded}}</td></tr>
	<tr><th>CallsFailed</th><td class="num">{{.Server.Data.CallsFailed}}</td></tr>
	<tr><th>LastCallStartedTimestamp</th><td class="mono">{{.Server.Data.LastCallStartedTimestamp | timestamp}}</td></tr>
</table>

<section class="section">
	<h2>Sockets <span class="count">{{.Server.ListenSocket | len}}</span></h2>
	{{template "chips-socket" .Server.ListenSocket}}
</section>

<section class="section">
	<h2>Events</h2>
	{{template "events" .Server.Data.Trace}}
</section>
`
