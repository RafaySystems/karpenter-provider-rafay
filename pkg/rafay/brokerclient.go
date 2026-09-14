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

// Ensure BrokerClient satisfies the compile-time check that StartBatcher is present.
var _ interface{ StartBatcher(context.Context) } = (*BrokerClient)(nil)

const (
	// gRPC metadata key for stream routing (edge-common/pkg/common.SessionID).
	grpcMetadataSessionID = "sessionid"

	// brokerCallTimeout bounds each individual broker RPC (send batch, status poll, cancel).
	// These calls are short request/response exchanges; long-running work is tracked via
	// status polling, so no call should outlive this deadline.
	brokerCallTimeout = 30 * time.Second

	// karpenterConfigCallTimeout bounds GetKarpenterConfig. It is more generous than
	// brokerCallTimeout because the broker fans out to PaaS for one compute-instance read plus
	// one compute-profile read per distinct node SKU, all on the caller's clock.
	karpenterConfigCallTimeout = 90 * time.Second

	defaultEdgeClientTLSPort = 5448
	defaultEdgeBrokerRPCPort = 5449
)

// BrokerClient talks to edge-broker over the same TLS/insecure dial as before; node add/remove use
// rep.edge.v1.KarpenterBatchService.BatchStreamOperations (not EdgeCommandService).
// It keeps one long-lived *grpc.ClientConn; each call opens a short-lived bidi stream on that connection.
type BrokerClient struct {
	CertPath     string
	KeyPath      string
	CAPath       string
	BrokerPort   int
	EdgeID       string
	StreamID     string
	DialHost     string
	GRPCInsecure bool

	mu      sync.Mutex
	conn    *grpc.ClientConn
	batcher *NodeBatcher
}

// NewBrokerClient creates a client. StreamID is gRPC metadata sessionid (edge-client: common.NewID()).
// EdgeID is informational; the broker takes edge id from the TLS client cert Subject O.
func NewBrokerClient(certPath, keyPath, caPath string, brokerPort int, edgeID, streamID, dialHost string, grpcInsecure bool) *BrokerClient {
	c := &BrokerClient{
		CertPath:     certPath,
		KeyPath:      keyPath,
		CAPath:       caPath,
		BrokerPort:   brokerPort,
		EdgeID:       edgeID,
		StreamID:     streamID,
		DialHost:     dialHost,
		GRPCInsecure: grpcInsecure,
	}
	c.batcher = NewNodeBatcher(c)
	return c
}

// StartBatcher launches the NodeBatcher goroutines (sender + poller). Call once from main after
// the BrokerClient is created; the batcher runs until ctx is cancelled.
func (c *BrokerClient) StartBatcher(ctx context.Context) {
	go c.batcher.Run(ctx)
}

// Batcher returns the NodeBatcher used by this client.
func (c *BrokerClient) Batcher() *NodeBatcher {
	return c.batcher
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

// callBrokerOnce calls fn with a connection but does not retry. Use for streaming operations
// that must not be re-issued (e.g. SendBatch, SendBatchRemove) since retrying could duplicate work.
// On Unavailable the connection is closed so the next call gets a fresh one.
func (c *BrokerClient) callBrokerOnce(ctx context.Context, fn func(cc *grpc.ClientConn) error) error {
	cc, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	err = fn(cc)
	if err != nil && shouldRedial(err) {
		c.closeConn()
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

// GetNode is not implemented via broker; CloudProvider falls back to the Kubernetes API.
func (c *BrokerClient) GetNode(ctx context.Context, providerID string) (*NodeInfo, error) {
	return nil, ErrGetNodeUnsupported
}

// ListNodes is not implemented via broker; CloudProvider falls back to listing Kubernetes Nodes.
func (c *BrokerClient) ListNodes(ctx context.Context, clusterID string) ([]*NodeInfo, error) {
	return nil, ErrListNodesUnsupported
}

// SendBatch sends a batch of node-add items to edge-broker via KarpenterBatchService.BatchStreamOperations.
// It opens a short-lived stream, sends the request, waits for KarpenterBatchAccepted, and returns the
// batch_id. The broker queues the batch for sequential processing. A broker-side rejection
// (queue full) is returned as ErrBatchRejected.
func (c *BrokerClient) SendBatch(ctx context.Context, nodes []*v1.KarpenterBatchNodeAddItem) (string, error) {
	if strings.TrimSpace(c.StreamID) == "" {
		return "", fmt.Errorf("STREAM_ID is required")
	}
	var batchID string
	err := c.callBrokerOnce(ctx, func(cc *grpc.ClientConn) error {
		callCtx, cancel := context.WithTimeout(c.streamContext(ctx), brokerCallTimeout)
		defer cancel()
		id, err := sendBatch(callCtx, cc, nodes)
		if err != nil {
			return err
		}
		batchID = id
		return nil
	})
	return batchID, err
}

// SendBatchRemove sends a batch of node-remove items to edge-broker via
// KarpenterBatchService.BatchStreamOperations. It opens a short-lived stream, sends the request,
// waits for KarpenterBatchAccepted, and returns the batch_id. The broker queues the batch for
// sequential processing. A broker-side rejection (queue full) is returned as ErrBatchRejected.
func (c *BrokerClient) SendBatchRemove(ctx context.Context, nodes []*v1.KarpenterBatchNodeRemoveItem) (string, error) {
	if strings.TrimSpace(c.StreamID) == "" {
		return "", fmt.Errorf("STREAM_ID is required")
	}
	var batchID string
	err := c.callBrokerOnce(ctx, func(cc *grpc.ClientConn) error {
		callCtx, cancel := context.WithTimeout(c.streamContext(ctx), brokerCallTimeout)
		defer cancel()
		id, err := sendBatchRemove(callCtx, cc, nodes)
		if err != nil {
			return err
		}
		batchID = id
		return nil
	})
	return batchID, err
}

// PollBatchStatus polls edge-broker for the current per-node status of a batch.
func (c *BrokerClient) PollBatchStatus(ctx context.Context, batchID string) ([]*v1.KarpenterBatchNodeResult, error) {
	if strings.TrimSpace(c.StreamID) == "" {
		return nil, fmt.Errorf("STREAM_ID is required")
	}
	var results []*v1.KarpenterBatchNodeResult
	err := c.callBroker(ctx, func(cc *grpc.ClientConn) error {
		callCtx, cancel := context.WithTimeout(c.streamContext(ctx), brokerCallTimeout)
		defer cancel()
		r, err := pollBatchStatus(callCtx, cc, batchID)
		if err != nil {
			return err
		}
		results = r
		return nil
	})
	return results, err
}

// GetKarpenterConfig fetches this cluster's node pools and node SKUs from edge-broker, rendered
// as RafayNodeClass and NodePool manifests (rep.edge.v1.KarpenterConfigService).
//
// The broker resolves the cluster from the mTLS client certificate; clusterID/projectID are
// sent for its logs only. Unlike the batch calls this is a plain unary RPC, so it goes through
// callBroker and gets the one-shot redial on Unavailable — a retried read is harmless.
func (c *BrokerClient) GetKarpenterConfig(ctx context.Context, clusterID, projectID string) (*v1.KarpenterConfigResponse, error) {
	if strings.TrimSpace(c.StreamID) == "" {
		return nil, fmt.Errorf("STREAM_ID is required")
	}
	var resp *v1.KarpenterConfigResponse
	err := c.callBroker(ctx, func(cc *grpc.ClientConn) error {
		callCtx, cancel := context.WithTimeout(c.streamContext(ctx), karpenterConfigCallTimeout)
		defer cancel()
		r, err := v1.NewKarpenterConfigServiceClient(cc).GetKarpenterConfig(callCtx, &v1.KarpenterConfigRequest{
			ClusterId: clusterID,
			ProjectId: projectID,
		})
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

// CancelOperations asks edge-broker to cancel the given operations (best-effort: only ops still
// ACCEPTED at the broker are cancelled). Returns the operation IDs actually cancelled.
func (c *BrokerClient) CancelOperations(ctx context.Context, operationIDs []string) ([]string, error) {
	if strings.TrimSpace(c.StreamID) == "" {
		return nil, fmt.Errorf("STREAM_ID is required")
	}
	var cancelled []string
	err := c.callBroker(ctx, func(cc *grpc.ClientConn) error {
		callCtx, cancel := context.WithTimeout(c.streamContext(ctx), brokerCallTimeout)
		defer cancel()
		ids, err := cancelOps(callCtx, cc, operationIDs)
		if err != nil {
			return err
		}
		cancelled = ids
		return nil
	})
	return cancelled, err
}
