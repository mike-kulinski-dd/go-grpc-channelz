package channelz

import (
	"context"
	"fmt"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// WriteChannelPage writes an HTML document to w containing per-channel RPC stats, including a header and a footer.
func (h *grpcChannelzHandler) WriteChannelPage(w io.Writer, channel int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ channel %d", channel))
	h.writeChannel(w, channel)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeChannel(w io.Writer, channel int64) {
	data := h.getChannel(channel)
	if data == nil {
		writeUnavailable(w)
		return
	}
	if err := channelTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getChannel(channelID int64) *channelzgrpc.GetChannelResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	channel, err := client.GetChannel(ctx, &channelzgrpc.GetChannelRequest{ChannelId: channelID})
	if err != nil {
		log.Errorf("Error querying GetChannel %+v", err)
		return nil
	}
	return channel
}

const channelTemplateHTML = `
<table class="detail">
    <tr><th>ChannelId</th><td class="mono">{{.Channel.Ref.ChannelId}}</td></tr>
    <tr><th>Channel Name</th><td class="mono">{{.Channel.Ref.Name}}</td></tr>
	<tr>
        <th>State</th>
        <td><span class="state {{.Channel.Data.State | stateClass}}">{{.Channel.Data.State}}</span></td>
	</tr>
	<tr>
        <th>Target</th>
        <td class="mono">
			{{.Channel.Data.Target}}
			{{if isXds .Channel.Data.Target}}
				&nbsp;[<a href="{{link "xds" "vhost" .Channel.Ref.ChannelId}}">view xDS config</a>]
			{{end}}
		</td>
	</tr>
	<tr><th>CreationTimestamp</th><td class="mono">{{.Channel.Data.Trace.CreationTimestamp | timestamp}}</td></tr>
	<tr><th>CallsStarted</th><td class="num">{{.Channel.Data.CallsStarted}}</td></tr>
	<tr><th>CallsSucceeded</th><td class="num">{{.Channel.Data.CallsSucceeded}}</td></tr>
	<tr><th>CallsFailed</th><td class="num">{{.Channel.Data.CallsFailed}}</td></tr>
	<tr><th>LastCallStartedTimestamp</th><td class="mono">{{.Channel.Data.LastCallStartedTimestamp | timestamp}}</td></tr>
</table>

<section class="section">
	<h2>Subchannels <span class="count">{{.Channel.SubchannelRef | len}}</span></h2>
	{{template "chips-subchannel" .Channel.SubchannelRef}}
</section>

<section class="section">
	<h2>Child Channels <span class="count">{{.Channel.ChannelRef | len}}</span></h2>
	{{template "chips-channel" .Channel.ChannelRef}}
</section>

<section class="section">
	<h2>Sockets <span class="count">{{.Channel.SocketRef | len}}</span></h2>
	{{template "chips-socket" .Channel.SocketRef}}
</section>

<section class="section">
	<h2>Events</h2>
	{{template "events" .Channel.Data.Trace}}
</section>
`
