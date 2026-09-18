package channelz

import (
	"context"
	"fmt"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

func (h *grpcChannelzHandler) WriteSocketPage(w io.Writer, socket int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ socket %d", socket))
	h.writeSocket(w, socket)
	writeFooter(w)
}

// writeSocket writes HTML to w containing RPC single socket stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeSocket(w io.Writer, socket int64) {
	data := h.getSocket(socket)
	if data == nil {
		writeUnavailable(w)
		return
	}
	if err := socketTemplate.Execute(w, data); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getSocket(socketID int64) *channelzgrpc.GetSocketResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	socket, err := client.GetSocket(ctx, &channelzgrpc.GetSocketRequest{SocketId: socketID})
	if err != nil {
		log.Errorf("Error querying GetSocket %+v", err)
		return nil
	}
	return socket
}

const socketTemplateHTML = `
<table class="detail">
    <tr><th>SocketId</th><td class="mono">{{.Socket.Ref.SocketId}}</td></tr>
	<tr><th>Socket Name</th><td class="mono">{{.Socket.Ref.Name}}</td></tr>
	<tr><th>Local address</th><td class="mono">{{.Socket.Local | address}}</td></tr>
	<tr>
		<th>Remote address</th>
		<td class="mono">{{.Socket.Remote | address}} {{with .Socket.RemoteName}}({{.}}){{end}}</td>
	</tr>
	<tr><th>StreamsStarted</th><td class="num">{{.Socket.Data.StreamsStarted}}</td></tr>
	<tr><th>StreamsSucceeded</th><td class="num">{{.Socket.Data.StreamsSucceeded}}</td></tr>
	<tr><th>StreamsFailed</th><td class="num">{{.Socket.Data.StreamsFailed}}</td></tr>
	<tr><th>MessagesSent</th><td class="num">{{.Socket.Data.MessagesSent}}</td></tr>
	<tr><th>MessagesReceived</th><td class="num">{{.Socket.Data.MessagesReceived}}</td></tr>
	<tr><th>KeepAlivesSent</th><td class="num">{{.Socket.Data.KeepAlivesSent}}</td></tr>
	<tr><th>LastLocalStreamCreated</th><td class="mono">{{.Socket.Data.LastLocalStreamCreatedTimestamp | timestamp}}</td></tr>
	<tr><th>LastRemoteStreamCreated</th><td class="mono">{{.Socket.Data.LastRemoteStreamCreatedTimestamp | timestamp}}</td></tr>
	<tr><th>LastMessageSent</th><td class="mono">{{.Socket.Data.LastMessageSentTimestamp | timestamp}}</td></tr>
	<tr><th>LastMessageReceived</th><td class="mono">{{.Socket.Data.LastMessageReceivedTimestamp | timestamp}}</td></tr>
	<tr><th>LocalFlowControlWindow</th><td class="num">{{.Socket.Data.LocalFlowControlWindow.Value}}</td></tr>
	<tr><th>RemoteFlowControlWindow</th><td class="num">{{.Socket.Data.RemoteFlowControlWindow.Value}}</td></tr>
	<tr>
		<th>Options</th>
		<td>
			{{if .Socket.Data.Option}}
			<div class="panel panel--options">
				{{range .Socket.Data.Option}}
					<div class="mono">{{.Name}}: {{.Value}} {{with .Additional}}({{.}}){{end}}</div>
				{{end}}
			</div>
			{{else}}
				<span class="empty">none</span>
			{{end}}
		</td>
	</tr>
	<tr><th>Security</th><td><div class="panel"><pre>{{.Socket.Security}}</pre></div></td></tr>
</table>
`
