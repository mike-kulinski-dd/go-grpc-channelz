// channelz-ui spins up an HTTP server that serves the go-grpc-channelz UI,
// pointed at a target gRPC server that has registered channelz (and optionally CSDS).
//
// Example:
//
//	channelz-ui --admin_port=50051 --http_port=8081
//
// Then open the printed URL.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"

	channelz "github.com/rantav/go-grpc-channelz"
)

func main() {
	adminPort := flag.Int("admin_port", 50051, "Port of the target gRPC server's admin services (channelz/CSDS)")
	adminHost := flag.String("admin_host", "localhost", "Host of the target gRPC server's admin services")
	httpPort := flag.Int("http_port", 8081, "Port on which to serve the channelz UI over HTTP")
	pathPrefix := flag.String("path_prefix", "/", "URL path prefix to mount the channelz UI under")
	flag.Parse()

	target := net.JoinHostPort(*adminHost, strconv.Itoa(*adminPort))
	httpAddr := ":" + strconv.Itoa(*httpPort)

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", httpAddr, err)
	}

	handler := channelz.CreateHandler(*pathPrefix, target)

	url := fmt.Sprintf("http://localhost:%d%schannelz/", *httpPort, normalizePrefix(*pathPrefix))
	log.Printf("Serving channelz UI for gRPC admin at %s", target)
	log.Printf("Open: %s", url)

	if err := http.Serve(listener, handler); err != nil {
		log.Fatalf("Failed to serve channelz UI: %v", err)
	}
}

// normalizePrefix returns the prefix with a trailing slash and no double slashes
// so the printed URL looks like http://localhost:8081/channelz/ regardless of input.
func normalizePrefix(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	if p[len(p)-1] != '/' {
		p = p + "/"
	}
	return p
}
