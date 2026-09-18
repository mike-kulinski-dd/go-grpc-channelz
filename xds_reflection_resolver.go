package channelz

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	log "google.golang.org/grpc/grpclog"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// protoResolver is what protoyaml/protojson need to expand google.protobuf.Any
// fields: a way to look up a message/extension type by name or type URL.
type protoResolver interface {
	protoregistry.ExtensionTypeResolver
	protoregistry.MessageTypeResolver
}

// loggedUnknownAnyURLs dedupes "couldn't resolve this Any type" warnings so we
// log each missing type once per process rather than once per page render.
var loggedUnknownAnyURLs sync.Map

// reflectionResolver resolves proto message types for google.protobuf.Any
// payloads by querying the target's own gRPC reflection service — the same
// approach grpcurl uses. The target already has every Envoy extension type it
// actually uses compiled in (it had to, to configure them), so its reflection
// response is comprehensive; we don't need to separately compile in and
// register every extension type ourselves, the way xds_proto_registry.go used
// to require.
type reflectionResolver struct {
	conn *grpc.ClientConn

	mu        sync.Mutex
	transport *reflectionV1Transport // nil until first successful use
	unusable  bool                   // true once we've given up (target has no reflection)
	files     *protoregistry.Files
	types     *protoregistry.Types
}

func newReflectionResolver(conn *grpc.ClientConn) *reflectionResolver {
	return &reflectionResolver{
		conn:  conn,
		files: &protoregistry.Files{},
		types: &protoregistry.Types{},
	}
}

// reflectionV1Transport wraps the (stable) gRPC reflection v1 protocol's
// bidi stream, serializing request/response round trips.
type reflectionV1Transport struct {
	mu     sync.Mutex
	stream reflectionv1.ServerReflection_ServerReflectionInfoClient
}

func newReflectionV1Transport(ctx context.Context, conn *grpc.ClientConn) (*reflectionV1Transport, error) {
	stream, err := reflectionv1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	return &reflectionV1Transport{stream: stream}, nil
}

func (t *reflectionV1Transport) fileContainingSymbol(symbol string) ([][]byte, error) {
	return t.roundTrip(&reflectionv1.ServerReflectionRequest{
		MessageRequest: &reflectionv1.ServerReflectionRequest_FileContainingSymbol{FileContainingSymbol: symbol},
	})
}

func (t *reflectionV1Transport) fileByFilename(filename string) ([][]byte, error) {
	return t.roundTrip(&reflectionv1.ServerReflectionRequest{
		MessageRequest: &reflectionv1.ServerReflectionRequest_FileByFilename{FileByFilename: filename},
	})
}

func (t *reflectionV1Transport) roundTrip(req *reflectionv1.ServerReflectionRequest) ([][]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.stream.Send(req); err != nil {
		return nil, err
	}
	resp, err := t.stream.Recv()
	if err != nil {
		return nil, err
	}
	if e := resp.GetErrorResponse(); e != nil {
		return nil, status.Error(codes.Code(e.GetErrorCode()), e.GetErrorMessage())
	}
	fd := resp.GetFileDescriptorResponse()
	if fd == nil {
		return nil, fmt.Errorf("channelz: unexpected reflection response %T", resp.GetMessageResponse())
	}
	return fd.GetFileDescriptorProto(), nil
}

// call issues a reflection request, lazily creating (and reusing) the stream
// to the target.
func (r *reflectionResolver) call(fn func(*reflectionV1Transport) ([][]byte, error)) ([][]byte, error) {
	r.mu.Lock()
	transport := r.transport
	unusable := r.unusable
	r.mu.Unlock()
	if unusable {
		return nil, fmt.Errorf("channelz: target does not support gRPC reflection")
	}

	if transport == nil {
		t, err := newReflectionV1Transport(context.Background(), r.conn)
		if err != nil {
			return nil, err
		}
		transport = t
	}

	data, err := fn(transport)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			r.mu.Lock()
			r.unusable = true
			r.mu.Unlock()
		}
		return nil, err
	}

	r.mu.Lock()
	r.transport = transport
	r.mu.Unlock()
	return data, nil
}

// resolveMessageType looks up a message type by fetching its descriptor (and
// any not-yet-known dependencies) from the target over gRPC reflection.
func (r *reflectionResolver) resolveMessageType(name protoreflect.FullName) (protoreflect.MessageType, error) {
	r.mu.Lock()
	mt, err := r.types.FindMessageByName(name)
	r.mu.Unlock()
	if err == nil {
		return mt, nil
	}

	raw, err := r.call(func(t *reflectionV1Transport) ([][]byte, error) {
		return t.fileContainingSymbol(string(name))
	})
	if err != nil {
		return nil, err
	}
	if err := r.registerFileDescriptors(raw); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.types.FindMessageByName(name)
}

func (r *reflectionResolver) registerFileDescriptors(raws [][]byte) error {
	batch := make(map[string]*descriptorpb.FileDescriptorProto, len(raws))
	for _, raw := range raws {
		fd := &descriptorpb.FileDescriptorProto{}
		if err := proto.Unmarshal(raw, fd); err != nil {
			return err
		}
		batch[fd.GetName()] = fd
	}
	for name, fd := range batch {
		if err := r.registerFileDescriptor(name, fd, batch); err != nil {
			return err
		}
	}
	return nil
}

// registerFileDescriptor recursively ensures fd's dependencies are built and
// registered first (fetching any not already in batch via FileByFilename),
// then builds and registers fd itself and every message type it defines.
func (r *reflectionResolver) registerFileDescriptor(name string, fd *descriptorpb.FileDescriptorProto, batch map[string]*descriptorpb.FileDescriptorProto) error {
	r.mu.Lock()
	_, err := r.files.FindFileByPath(name)
	r.mu.Unlock()
	if err == nil {
		return nil
	}

	for _, dep := range fd.GetDependency() {
		r.mu.Lock()
		_, derr := r.files.FindFileByPath(dep)
		r.mu.Unlock()
		if derr == nil {
			continue
		}
		depFd, ok := batch[dep]
		if !ok {
			raw, ferr := r.call(func(t *reflectionV1Transport) ([][]byte, error) {
				return t.fileByFilename(dep)
			})
			if ferr != nil {
				return fmt.Errorf("channelz: fetching reflection dependency %q: %w", dep, ferr)
			}
			for _, rb := range raw {
				dfd := &descriptorpb.FileDescriptorProto{}
				if uerr := proto.Unmarshal(rb, dfd); uerr != nil {
					return uerr
				}
				batch[dfd.GetName()] = dfd
			}
			depFd, ok = batch[dep]
			if !ok {
				return fmt.Errorf("channelz: reflection server did not return dependency %q", dep)
			}
		}
		if err := r.registerFileDescriptor(dep, depFd, batch); err != nil {
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.files.FindFileByPath(name); err == nil {
		return nil // registered by a recursive call above while we were fetching deps
	}
	fileDesc, err := protodesc.NewFile(fd, r.files)
	if err != nil {
		return fmt.Errorf("channelz: building descriptor for %q: %w", name, err)
	}
	if err := r.files.RegisterFile(fileDesc); err != nil {
		return err
	}
	registerMessageTypes(r.types, fileDesc.Messages())
	return nil
}

func registerMessageTypes(types *protoregistry.Types, msgs protoreflect.MessageDescriptors) {
	for i := range msgs.Len() {
		md := msgs.Get(i)
		_ = types.RegisterMessage(dynamicpb.NewMessageType(md))
		registerMessageTypes(types, md.Messages())
	}
}

func (r *reflectionResolver) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	if mt, err := protoregistry.GlobalTypes.FindMessageByName(name); err == nil {
		return mt, nil
	}
	return r.resolveMessageType(name)
}

func (r *reflectionResolver) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	name := url
	if i := strings.LastIndexByte(url, '/'); i >= 0 {
		name = url[i+1:]
	}
	if mt, err := r.FindMessageByName(protoreflect.FullName(name)); err == nil {
		return mt, nil
	}
	if _, loaded := loggedUnknownAnyURLs.LoadOrStore(url, struct{}{}); !loaded {
		log.Warningf("channelz: could not resolve Any type %q via gRPC reflection; rendering as @type-only", url)
	}
	return (&emptypb.Empty{}).ProtoReflect().Type(), nil
}

func (r *reflectionResolver) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	return protoregistry.GlobalTypes.FindExtensionByName(field)
}

func (r *reflectionResolver) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	return protoregistry.GlobalTypes.FindExtensionByNumber(message, field)
}
