package channelz

import (
	"context"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// WriteChannelsPage writes an HTML document to w containing per-channel RPC stats, including a header and a footer.
func (h *grpcChannelzHandler) WriteChannelsPage(w io.Writer, start int64) {
	writeHeader(w, "Channels")
	h.writeChannels(w, start)
	writeFooter(w)
}

// writeTopChannels writes HTML to w containing per-channel RPC stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeChannels(w io.Writer, start int64) {
	data := h.getTopChannels(start)
	if data == nil {
		writeUnavailable(w)
		return
	}
	if err := channelsTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getTopChannels(start int64) *channelzgrpc.GetTopChannelsResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	channels, err := client.GetTopChannels(ctx, &channelzgrpc.GetTopChannelsRequest{
		StartChannelId: start,
	})
	if err != nil {
		log.Errorf("Error querying GetTopChannels %+v", err)
		return nil
	}
	return channels
}

const channelsTemplateHTML = `
{{define "channel-header"}}
    <tr>
        <th>Channel</th>
        <th>State</th>
        <th>Target</th>
        <th>Subchannels</th>
        <th>Child Channels</th>
        <th>Sockets</th>
        <th>CreationTimestamp</th>
        <th class="num">CallsStarted</th>
        <th class="num">CallsSucceeded</th>
        <th class="num">CallsFailed</th>
        <th>LastCallStartedTimestamp</th>
    </tr>
{{end}}

{{define "channel-body"}}
    <tr>
        <td class="mono"><a href="{{link "channel" .Ref.ChannelId}}"><b>{{.Ref.ChannelId}}</b> {{.Ref.Name}}</a></td>
        <td><span class="state {{.Data.State | stateClass}}">{{.Data.State}}</span></td>
        <td class="mono">{{.Data.Target}}</td>
		<td class="num">{{.SubchannelRef | len}}</td>
		<td class="chips">
			{{range .ChannelRef}}
				<a class="chip" href="{{link "channel" .ChannelId}}"><b>{{.ChannelId}}</b><span class="name">{{.Name}}</span></a>
			{{end}}
		</td>
		<td class="chips">
			{{range .SocketRef}}
				<a class="chip" href="{{link "socket" .SocketId}}"><b>{{.SocketId}}</b><span class="name">{{.Name}}</span></a>
			{{end}}
		</td>
        <td class="mono">{{.Data.Trace.CreationTimestamp | timestamp}}</td>
        <td class="num">{{.Data.CallsStarted}}</td>
        <td class="num">{{.Data.CallsSucceeded}}</td>
        <td class="num">{{.Data.CallsFailed}}</td>
        <td class="mono">{{.Data.LastCallStartedTimestamp | timestamp}}</td>
	</tr>
{{end}}

<section class="section">
	<h2>Clients: {{.Channel | len}}</h2>
	<table class="grid">
		<thead>
			{{template "channel-header"}}
		</thead>
		<tbody>
			{{$last := ""}}
			{{range .Channel}}
				{{template "channel-body" .}}
				{{$last = .}}
			{{end}}
		</tbody>
	</table>
	{{if and .Channel (not .End)}}
		<nav class="pager">
			<a href="{{link "channels"}}?start={{$last.Ref.ChannelId}}">Next&nbsp;&gt;</a>
		</nav>
	{{end}}
</section>
`
