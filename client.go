package channelz

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	csdsgrpc "github.com/envoyproxy/go-control-plane/envoy/service/status/v3"
	"google.golang.org/grpc"
	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	"google.golang.org/grpc/connectivity"
)

func (h *grpcChannelzHandler) connect() (channelzgrpc.ChannelzClient, error) {
	if h.injectedClient() {
		// client was set directly (e.g. injected by tests) without a
		// managed connection for dial() to (re)check; use it as-is.
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.client, nil
	}
	if err := h.dial(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.client, nil
}

func (h *grpcChannelzHandler) connectCSDS() (csdsgrpc.ClientStatusDiscoveryServiceClient, error) {
	if h.injectedClient() {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.csdsClient, nil
	}
	if err := h.dial(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.csdsClient, nil
}

// injectedClient reports whether a client/csdsClient was set directly
// (e.g. by tests) without a managed connection, so dial() should be
// bypassed entirely rather than attempting to redial one.
func (h *grpcChannelzHandler) injectedClient() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conn == nil && (h.client != nil || h.csdsClient != nil)
}

// reflectionResolver returns the lazily-created gRPC-reflection-backed proto
// type resolver for this handler's connection, or nil if not yet dialed.
func (h *grpcChannelzHandler) reflectionResolver() protoResolver {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn == nil {
		return nil
	}
	if h.reflResolver == nil {
		h.reflResolver = newReflectionResolver(h.conn)
	}
	return h.reflResolver
}

func (h *grpcChannelzHandler) dial() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn != nil {
		switch h.conn.GetState() {
		case connectivity.TransientFailure, connectivity.Shutdown:
			// The connection is broken and grpc-go's own backoff may not
			// retry for a while yet; tear it down so we dial a fresh
			// connection below instead of returning a stub bound to a
			// connection that will keep failing every RPC.
			h.conn.Close()
			h.conn = nil
			h.client = nil
			h.csdsClient = nil
			h.reflResolver = nil
		default:
			return nil
		}
	}
	host := getHostFromBindAddress(h.bindAddress)
	conn, err := grpc.Dial(host, h.dialOpts...)
	if err != nil {
		return errors.Wrapf(err, "error dialing to %s", host)
	}
	h.conn = conn
	h.client = channelzgrpc.NewChannelzClient(conn)
	h.csdsClient = csdsgrpc.NewClientStatusDiscoveryServiceClient(conn)
	return nil
}

func getHostFromBindAddress(bindAddress string) string {
	if strings.HasPrefix(bindAddress, ":") {
		return fmt.Sprintf("localhost%s", bindAddress)
	}
	return bindAddress
}
