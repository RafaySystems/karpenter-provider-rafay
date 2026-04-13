/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rafay

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/broker"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/brokerproto"
	"k8s.io/klog/v2"
)

const (
	// Same bidi RPC as edge-client: rep.edge.v1.EdgeCommandService.Execute
	edgeCommandExecutePath = "/rep.edge.v1.EdgeCommandService/Execute"
	// gRPC metadata key for stream routing (edge-common/pkg/common.SessionID).
	grpcMetadataSessionID = "sessionid"

	pollTimeout = 15 * time.Minute

	defaultEdgeClientTLSPort = 5448
	defaultEdgeBrokerRPCPort = 5449
)

var edgeCommandStreamDesc = &grpc.StreamDesc{
	StreamName:    "Execute",
	ClientStreams: true,
	ServerStreams: true,
}

// BrokerClient talks to edge-broker using EdgeCommandService (same secured dial and
// EdgeCommand messages as edge-client), not EdgeBrokerService.
// It keeps one long-lived *grpc.ClientConn; each add/remove opens a short bidi stream on that connection.
type BrokerClient struct {
	CertPath     string
	KeyPath      string
	CAPath       string
	BrokerPort   int
	EdgeID       string
	StreamID     string
	DialHost     string
	GRPCInsecure bool

	mu   sync.Mutex
	conn *grpc.ClientConn
}

// NewBrokerClient creates a client. StreamID is gRPC metadata sessionid (edge-client: common.NewID()).
// EdgeID is informational; the broker takes edge id from the TLS client cert Subject O.
func NewBrokerClient(certPath, keyPath, caPath string, brokerPort int, edgeID, streamID, dialHost string, grpcInsecure bool) *BrokerClient {
	return &BrokerClient{
		CertPath:     certPath,
		KeyPath:      keyPath,
		CAPath:       caPath,
		BrokerPort:   brokerPort,
		EdgeID:       edgeID,
		StreamID:     streamID,
		DialHost:     dialHost,
		GRPCInsecure: grpcInsecure,
	}
}

// dialBroker creates a new gRPC connection: TLS (edge-client–compatible) unless GRPCInsecure.
func (c *BrokerClient) dialBroker(ctx context.Context) (*grpc.ClientConn, error) {
	host := strings.TrimSpace(c.DialHost)
	if host == "" {
		if c.CertPath == "" {
			return nil, fmt.Errorf("broker dial host: set EDGE_BROKER_GRPC_HOST or CERT_FOLDER for host from client.crt OU")
		}
		h, err := broker.GetServerHostFromCert(c.CertPath)
		if err != nil {
			return nil, fmt.Errorf("broker dial host: %w", err)
		}
		host = h
	}

	port := c.BrokerPort
	if port <= 0 {
		if c.GRPCInsecure {
			port = defaultEdgeBrokerRPCPort
		} else {
			port = defaultEdgeClientTLSPort
		}
	}

	var conn *grpc.ClientConn
	var err error
	if c.GRPCInsecure {
		conn, err = broker.NewInsecureGrpcClientConn(ctx, host, port)
	} else {
		creds, tlsHost, credErr := broker.GetEdgeClientCredentials(c.CertPath, c.KeyPath, c.CAPath)
		if credErr != nil {
			return nil, fmt.Errorf("broker credentials: %w", credErr)
		}
		if strings.TrimSpace(c.DialHost) == "" {
			host = tlsHost
		}
		conn, err = broker.NewSecureGrpcClientConn(ctx, host, port, creds)
	}
	if err != nil {
		klog.Errorf("edge-broker: gRPC dial failed addr=%s:%d insecure=%t: %v", host, port, c.GRPCInsecure, err)
		return nil, err
	}
	klog.Infof("edge-broker: gRPC dial succeeded addr=%s:%d insecure=%t", host, port, c.GRPCInsecure)
	return conn, nil
}

func (c *BrokerClient) getConn(ctx context.Context) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		klog.V(3).Info("edge-broker: reusing existing gRPC connection")
		return c.conn, nil
	}
	conn, err := c.dialBroker(ctx)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return c.conn, nil
}

func (c *BrokerClient) closeConnLocked() {
	if c.conn != nil {
		klog.V(2).Info("edge-broker: closing gRPC connection")
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *BrokerClient) closeConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeConnLocked()
}

func shouldRedial(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.Unavailable
}

func (c *BrokerClient) callBroker(ctx context.Context, fn func(cc *grpc.ClientConn) error) error {
	cc, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	err = fn(cc)
	if err != nil && shouldRedial(err) {
		klog.Warningf("edge-broker: RPC failed (%v), closing connection and retrying once", err)
		c.closeConn()
		cc2, err2 := c.getConn(ctx)
		if err2 != nil {
			return err2
		}
		err = fn(cc2)
	}
	return err
}

func (c *BrokerClient) streamContext(ctx context.Context) context.Context {
	sid := strings.TrimSpace(c.StreamID)
	if sid == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, grpcMetadataSessionID, sid)
}

// AddNodes sends KarpenterAddNodeRequest on EdgeCommandService.Execute and waits for the matching response.
func (c *BrokerClient) AddNodes(ctx context.Context, req AddNodesRequest) (*AddNodesResponse, error) {
	if strings.TrimSpace(c.StreamID) == "" {
		return nil, fmt.Errorf("STREAM_ID is required (gRPC metadata sessionid for edge-broker)")
	}
	var out *AddNodesResponse
	err := c.callBroker(ctx, func(cc *grpc.ClientConn) error {
		rid := uuid.NewString()
		sctx := c.streamContext(ctx)
		deadline := time.Now().Add(pollTimeout)
		return runEdgeCommandStream(sctx, cc, func(stream grpc.ClientStream) error {
			cmd := &brokerproto.EdgeCommand{
				CommandType: brokerproto.EdgeCommandType_KarpenterAddNode,
				MessageType: brokerproto.EdgeMessageType_KarpenterAddNodeRequest,
				Rid:         rid,
				KarpenterAddNodeCommand: &brokerproto.KarpenterAddNodeCommand{
					EdgeId:       strings.TrimSpace(c.EdgeID),
					StreamId:     strings.TrimSpace(c.StreamID),
					ClusterId:    req.ClusterID,
					ProjectId:    req.ProjectID,
					InstanceType: req.InstanceType,
					NodePoolName: req.NodePoolName,
				},
			}
			if err := stream.SendMsg(cmd); err != nil {
				klog.Errorf("edge-broker: KarpenterAddNode SendMsg failed rid=%s: %v", rid, err)
				return err
			}
			klog.Infof("edge-broker: KarpenterAddNode request sent rid=%s instanceType=%q nodePool=%q", rid, req.InstanceType, req.NodePoolName)
			for time.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					klog.Warningf("edge-broker: KarpenterAddNode context cancelled rid=%s: %v", rid, ctx.Err())
					return ctx.Err()
				default:
				}
				msg := &brokerproto.EdgeCommand{}
				if err := stream.RecvMsg(msg); err != nil {
					klog.Errorf("edge-broker: KarpenterAddNode RecvMsg rid=%s: %v", rid, err)
					return err
				}
				if msg.GetRid() != rid {
					klog.V(4).Infof("edge-broker: KarpenterAddNode skipping message rid=%s (want %s)", msg.GetRid(), rid)
					continue
				}
				if msg.GetMessageType() != brokerproto.EdgeMessageType_KarpenterAddNodeResponse {
					klog.V(4).Infof("edge-broker: KarpenterAddNode skipping message type %v (want AddNodeResponse)", msg.GetMessageType())
					continue
				}
				if msg.GetStatus() == brokerproto.Status_Failed {
					return fmt.Errorf("broker add node failed: %s", msg.GetData())
				}
				pl := &brokerproto.AddNodeResponsePayload{}
				if d := msg.GetData(); d != "" {
					if err := proto.Unmarshal([]byte(d), pl); err != nil {
						return fmt.Errorf("unmarshal add-node response: %w", err)
					}
				}
				if len(pl.GetProviderIds()) == 0 {
					return fmt.Errorf("broker add node: no provider IDs in response")
				}
				out = &AddNodesResponse{ProviderIDs: pl.GetProviderIds()}
				klog.Infof("edge-broker: KarpenterAddNode response ok rid=%s providerIDs=%v", rid, pl.GetProviderIds())
				return nil
			}
			klog.Warningf("edge-broker: KarpenterAddNode timed out rid=%s after %v (no matching response)", rid, pollTimeout)
			return fmt.Errorf("broker add node: timeout after %v", pollTimeout)
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RemoveNode sends KarpenterRemoveNodeRequest on EdgeCommandService.Execute.
func (c *BrokerClient) RemoveNode(ctx context.Context, providerID string) error {
	if strings.TrimSpace(c.StreamID) == "" {
		return fmt.Errorf("STREAM_ID is required (gRPC metadata sessionid for edge-broker)")
	}
	return c.callBroker(ctx, func(cc *grpc.ClientConn) error {
		rid := uuid.NewString()
		sctx := c.streamContext(ctx)
		deadline := time.Now().Add(pollTimeout)
		return runEdgeCommandStream(sctx, cc, func(stream grpc.ClientStream) error {
			cmd := &brokerproto.EdgeCommand{
				CommandType: brokerproto.EdgeCommandType_KarpenterRemoveNode,
				MessageType: brokerproto.EdgeMessageType_KarpenterRemoveNodeRequest,
				Rid:         rid,
				KarpenterRemoveNodeCommand: &brokerproto.KarpenterRemoveNodeCommand{
					EdgeId:     strings.TrimSpace(c.EdgeID),
					StreamId:   strings.TrimSpace(c.StreamID),
					ProviderId: providerID,
				},
			}
			if err := stream.SendMsg(cmd); err != nil {
				klog.Errorf("edge-broker: KarpenterRemoveNode SendMsg failed rid=%s: %v", rid, err)
				return err
			}
			klog.Infof("edge-broker: KarpenterRemoveNode request sent rid=%s providerID=%q", rid, providerID)
			for time.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					klog.Warningf("edge-broker: KarpenterRemoveNode context cancelled rid=%s: %v", rid, ctx.Err())
					return ctx.Err()
				default:
				}
				msg := &brokerproto.EdgeCommand{}
				if err := stream.RecvMsg(msg); err != nil {
					klog.Errorf("edge-broker: KarpenterRemoveNode RecvMsg rid=%s: %v", rid, err)
					return err
				}
				if msg.GetRid() != rid {
					klog.V(4).Infof("edge-broker: KarpenterRemoveNode skipping message rid=%s (want %s)", msg.GetRid(), rid)
					continue
				}
				if msg.GetMessageType() != brokerproto.EdgeMessageType_KarpenterRemoveNodeResponse {
					klog.V(4).Infof("edge-broker: KarpenterRemoveNode skipping message type %v (want RemoveNodeResponse)", msg.GetMessageType())
					continue
				}
				if msg.GetStatus() == brokerproto.Status_Failed {
					return fmt.Errorf("broker remove node failed: %s", msg.GetData())
				}
				klog.Infof("edge-broker: KarpenterRemoveNode response ok rid=%s", rid)
				return nil
			}
			klog.Warningf("edge-broker: KarpenterRemoveNode timed out rid=%s after %v (no matching response)", rid, pollTimeout)
			return fmt.Errorf("broker remove node: timeout after %v", pollTimeout)
		})
	})
}

func runEdgeCommandStream(ctx context.Context, cc *grpc.ClientConn, fn func(grpc.ClientStream) error) error {
	stream, err := cc.NewStream(ctx, edgeCommandStreamDesc, edgeCommandExecutePath)
	if err != nil {
		klog.Errorf("edge-broker: NewStream %s failed: %v", edgeCommandExecutePath, err)
		return err
	}
	klog.V(2).Infof("edge-broker: opened bidi stream %s", edgeCommandExecutePath)
	defer func() { _ = stream.CloseSend() }()
	return fn(stream)
}

// GetNode is not implemented via broker; CloudProvider falls back to the Kubernetes API.
func (c *BrokerClient) GetNode(ctx context.Context, providerID string) (*NodeInfo, error) {
	return nil, ErrGetNodeUnsupported
}

// ListNodes is not implemented via broker; CloudProvider falls back to listing Kubernetes Nodes.
func (c *BrokerClient) ListNodes(ctx context.Context, clusterID string) ([]*NodeInfo, error) {
	return nil, ErrListNodesUnsupported
}
