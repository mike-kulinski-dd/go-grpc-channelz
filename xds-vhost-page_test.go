package channelz

import (
	"testing"

	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"github.com/stretchr/testify/assert"
)

func TestFindVirtualHostForChannel(t *testing.T) {
	r := &xdsResources{virtualHosts: map[string]*routev3.VirtualHost{
		"sidecar/http2/echo.workloads.dash":  {},
		"sidecar/http2/other.workloads.dash": {},
	}}
	assert.Equal(t, "sidecar/http2/echo.workloads.dash", findVirtualHostForChannel(r, "echo.workloads.dash"))

	// A lone call-time vhost isn't the channel's dial-time one.
	lone := &xdsResources{virtualHosts: map[string]*routev3.VirtualHost{"sidecar/http2/other.workloads.dash": {}}}
	assert.Empty(t, findVirtualHostForChannel(lone, "echo.workloads.dash"))
}
