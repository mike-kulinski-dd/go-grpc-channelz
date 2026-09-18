package channelz

import (
	"context"
	"fmt"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// WriteSubchannelsPage writes an HTML document to w containing per-channel RPC stats, including a header and a footer.
func (h *grpcChannelzHandler) WriteSubchannelPage(w io.Writer, subchannel int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ subchannel %d", subchannel))
	h.writeSubchannel(w, subchannel)
	writeFooter(w)
}

// writeSubchannel writes HTML to w containing sub-channel RPC stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeSubchannel(w io.Writer, subchannel int64) {
	data := h.getSubchannel(subchannel)
	if data == nil {
		writeUnavailable(w)
		return
	}
	if err := subChannelTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getSubchannel(subchannelID int64) *channelzgrpc.GetSubchannelResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	subchannel, err := client.GetSubchannel(ctx, &channelzgrpc.GetSubchannelRequest{
		SubchannelId: subchannelID,
	})
	if err != nil {
		log.Errorf("Error querying GetSubchannel %+v", err)
		return nil
	}
	return subchannel
}

const subChannelsTemplateHTML = `
<table class="detail">
    <tr>
		<th>Subchannel</th>
        <td class="mono">
			<a href="{{link "subchannel" .Subchannel.Ref.SubchannelId}}">
				<b>{{.Subchannel.Ref.SubchannelId}}</b> {{.Subchannel.Ref.Name}}
			</a>
		</td>
	</tr>
	<tr>
        <th>State</th>
        <td><span class="state {{.Subchannel.Data.State | stateClass}}">{{.Subchannel.Data.State}}</span></td>
	</tr>
	<tr><th>Target</th><td class="mono">{{.Subchannel.Data.Target}}</td></tr>
	<tr><th>CreationTimestamp</th><td class="mono">{{.Subchannel.Data.Trace.CreationTimestamp | timestamp}}</td></tr>
	<tr><th>CallsStarted</th><td class="num">{{.Subchannel.Data.CallsStarted}}</td></tr>
	<tr><th>CallsSucceeded</th><td class="num">{{.Subchannel.Data.CallsSucceeded}}</td></tr>
	<tr><th>CallsFailed</th><td class="num">{{.Subchannel.Data.CallsFailed}}</td></tr>
	<tr><th>LastCallStartedTimestamp</th><td class="mono">{{.Subchannel.Data.LastCallStartedTimestamp | timestamp}}</td></tr>
</table>

<section class="section">
	<h2>Child Channels <span class="count">{{.Subchannel.ChannelRef | len}}</span></h2>
	{{template "chips-channel" .Subchannel.ChannelRef}}
</section>

<section class="section">
	<h2>Child Subchannels <span class="count">{{.Subchannel.SubchannelRef | len}}</span></h2>
	{{template "chips-subchannel" .Subchannel.SubchannelRef}}
</section>

<section class="section">
	<h2>Socket <span class="count">{{.Subchannel.SocketRef | len}}</span></h2>
	{{template "chips-socket" .Subchannel.SocketRef}}
</section>

<section class="section">
	<h2>Events</h2>
	{{template "events" .Subchannel.Data.Trace}}
</section>
`
