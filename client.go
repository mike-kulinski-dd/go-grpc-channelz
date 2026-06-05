package channelz

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	csdsgrpc "github.com/envoyproxy/go-control-plane/envoy/service/status/v3"
	"google.golang.org/grpc"
	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
)

func (h *grpcChannelzHandler) connect() (channelzgrpc.ChannelzClient, error) {
	if h.client != nil {
		return h.client, nil
	}
	if err := h.dial(); err != nil {
		return nil, err
	}
	return h.client, nil
}

func (h *grpcChannelzHandler) connectCSDS() (csdsgrpc.ClientStatusDiscoveryServiceClient, error) {
	if h.csdsClient != nil {
		return h.csdsClient, nil
	}
	if err := h.dial(); err != nil {
		return nil, err
	}
	return h.csdsClient, nil
}

func (h *grpcChannelzHandler) dial() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn != nil {
		return nil
	}
	host := getHostFromBindAddress(h.bindAddress)
	conn, err := grpc.Dial(host, h.dialOpts...)
	if err != nil {
		return errors.Wrapf(err, "error dialing to %s", host)
	}
	h.conn = conn
	if h.client == nil {
		h.client = channelzgrpc.NewChannelzClient(conn)
	}
	if h.csdsClient == nil {
		h.csdsClient = csdsgrpc.NewClientStatusDiscoveryServiceClient(conn)
	}
	return nil
}

func getHostFromBindAddress(bindAddress string) string {
	if strings.HasPrefix(bindAddress, ":") {
		return fmt.Sprintf("localhost%s", bindAddress)
	}
	return bindAddress
}
