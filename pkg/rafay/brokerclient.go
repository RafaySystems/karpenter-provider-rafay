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

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/broker"
	"k8s.io/klog/v2"
)

const (
	// gRPC metadata key for stream routing (edge-common/pkg/common.SessionID).
	grpcMetadataSessionID = "sessionid"

	pollTimeout = 60 * time.Minute

	defaultEdgeClientTLSPort = 5448
	defaultEdgeBrokerRPCPort = 5449
)

// BrokerClient talks to edge-broker over the same TLS/insecure dial as before; node add/remove use
// rep.edge.v1.KarpenterNodeService.StreamOperations (not EdgeCommandService).
// It keeps one long-lived *grpc.ClientConn; each add/remove opens a dedicated bidi stream on that connection.
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

// AddNodes uses KarpenterNodeService.StreamOperations: node_add then status polls until provider IDs are returned.
func (c *BrokerClient) AddNodes(ctx context.Context, req AddNodesRequest) (*AddNodesResponse, error) {
	klog.Info("AddNodes", "pollTimeout", pollTimeout, "streamID", c.StreamID, "operationID", req.OperationID, "instanceType", req.InstanceType, "nodePool", req.NodePoolName, "clusterID", req.ClusterID, "projectID", req.ProjectID)
	if strings.TrimSpace(c.StreamID) == "" {
		return nil, fmt.Errorf("STREAM_ID is required (gRPC metadata sessionid for edge-broker)")
	}
	var out *AddNodesResponse
	err := c.callBroker(ctx, func(cc *grpc.ClientConn) error {
		pollCtx, cancel := context.WithTimeout(c.streamContext(ctx), pollTimeout)
		defer cancel()
		addReq := &v1.KarpenterNodeAddRequest{
			ClusterId:    req.ClusterID,
			ProjectId:    req.ProjectID,
			InstanceType: req.InstanceType,
			NodePoolName: req.NodePoolName,
		}
		if strings.TrimSpace(req.OperationID) == "" {
			return fmt.Errorf("OperationID is required for Karpenter node add (broker requires operation_id)")
		}
		klog.Infof("edge-broker: KarpenterNode add stream instanceType=%q nodePool=%q operationID=%q", req.InstanceType, req.NodePoolName, req.OperationID)
		ids, err := streamKarpenterNodeAdd(pollCtx, cc, strings.TrimSpace(c.StreamID), strings.TrimSpace(req.OperationID), addReq)
		if err != nil {
			klog.Errorf("edge-broker: KarpenterNode add stream failed: %v", err)
			return err
		}
		if len(ids) == 0 {
			return fmt.Errorf("broker add node: no provider IDs in response")
		}
		out = &AddNodesResponse{ProviderIDs: ids}
		klog.Infof("edge-broker: KarpenterNode add ok providerIDs=%v", ids)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RemoveNode uses KarpenterNodeService.StreamOperations: node_delete then status polls until success.
func (c *BrokerClient) RemoveNode(ctx context.Context, providerID, operationID string) error {
	if strings.TrimSpace(c.StreamID) == "" {
		return fmt.Errorf("STREAM_ID is required (gRPC metadata sessionid for edge-broker)")
	}
	if strings.TrimSpace(operationID) == "" {
		return fmt.Errorf("operationID is required for Karpenter node delete (broker requires operation_id)")
	}
	return c.callBroker(ctx, func(cc *grpc.ClientConn) error {
		pollCtx, cancel := context.WithTimeout(c.streamContext(ctx), pollTimeout)
		defer cancel()
		delReq := &v1.KarpenterNodeDeleteRequest{ProviderId: providerID}
		klog.Infof("edge-broker: KarpenterNode delete stream providerID=%q operationID=%q", providerID, operationID)
		if err := streamKarpenterNodeDelete(pollCtx, cc, strings.TrimSpace(c.StreamID), strings.TrimSpace(operationID), delReq); err != nil {
			klog.Errorf("edge-broker: KarpenterNode delete stream failed: %v", err)
			return err
		}
		klog.Infof("edge-broker: KarpenterNode delete ok providerID=%q", providerID)
		return nil
	})
}

// GetNode is not implemented via broker; CloudProvider falls back to the Kubernetes API.
func (c *BrokerClient) GetNode(ctx context.Context, providerID string) (*NodeInfo, error) {
	return nil, ErrGetNodeUnsupported
}

// ListNodes is not implemented via broker; CloudProvider falls back to listing Kubernetes Nodes.
func (c *BrokerClient) ListNodes(ctx context.Context, clusterID string) ([]*NodeInfo, error) {
	return nil, ErrListNodesUnsupported
}
