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
	"time"

	"github.com/RafaySystems/edge-common/pkg/common"
	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	karpenterNodePollInitialDelay = 120 * time.Second
	karpenterNodePollInterval     = 30 * time.Second
)

func waitOrCtxDone(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// streamKarpenterNodeAdd opens KarpenterNodeService.StreamOperations: sends node_add, then polls with
// status until the broker returns provider IDs (rep.edge.v1.KarpenterNodeService).
// operationID is required and is sent as KarpenterNodeStreamClientToBroker.operation_id (broker rejects empty).
func streamKarpenterNodeAdd(ctx context.Context, conn *grpc.ClientConn, sessionID, operationID string, req *v1.KarpenterNodeAddRequest) ([]string, error) {
	if req == nil {
		return nil, fmt.Errorf("nil KarpenterNodeAddRequest")
	}
	op := strings.TrimSpace(operationID)
	if op == "" {
		return nil, fmt.Errorf("operation_id is required")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, common.SessionID, sessionID)
	cli := v1.NewKarpenterNodeServiceClient(conn)
	stream, err := cli.StreamOperations(ctx)
	if err != nil {
		return nil, err
	}
	first := &v1.KarpenterNodeStreamClientToBroker{
		OperationId: op,
		Payload:     &v1.KarpenterNodeStreamClientToBroker_NodeAdd{NodeAdd: req},
	}
	if err := stream.Send(first); err != nil {
		return nil, err
	}
	opID, err := recvAcceptedOperationID(stream, op)
	if err != nil {
		return nil, err
	}
	if err := waitOrCtxDone(ctx, karpenterNodePollInitialDelay); err != nil {
		return nil, err
	}
	for {
		if err := stream.Send(&v1.KarpenterNodeStreamClientToBroker{
			OperationId: opID,
			Payload:     &v1.KarpenterNodeStreamClientToBroker_Status{Status: &v1.KarpenterNodeStatusPoll{}},
		}); err != nil {
			return nil, err
		}
		ids, done, ferr := recvStatusOrAddResult(stream)
		if ferr != nil {
			return nil, ferr
		}
		if done {
			return ids, nil
		}
		if err := waitOrCtxDone(ctx, karpenterNodePollInterval); err != nil {
			return nil, err
		}
	}
}

// streamKarpenterNodeDelete opens KarpenterNodeService.StreamOperations for node_delete + status polls.
// operationID is required (broker rejects empty).
func streamKarpenterNodeDelete(ctx context.Context, conn *grpc.ClientConn, sessionID, operationID string, req *v1.KarpenterNodeDeleteRequest) error {
	if req == nil {
		return fmt.Errorf("nil KarpenterNodeDeleteRequest")
	}
	op := strings.TrimSpace(operationID)
	if op == "" {
		return fmt.Errorf("operation_id is required")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, common.SessionID, sessionID)
	cli := v1.NewKarpenterNodeServiceClient(conn)
	stream, err := cli.StreamOperations(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.KarpenterNodeStreamClientToBroker{
		OperationId: op,
		Payload:     &v1.KarpenterNodeStreamClientToBroker_NodeDelete{NodeDelete: req},
	}); err != nil {
		return err
	}
	opID, err := recvAcceptedOperationID(stream, op)
	if err != nil {
		return err
	}
	if err := waitOrCtxDone(ctx, karpenterNodePollInitialDelay); err != nil {
		return err
	}
	for {
		if err := stream.Send(&v1.KarpenterNodeStreamClientToBroker{
			OperationId: opID,
			Payload:     &v1.KarpenterNodeStreamClientToBroker_Status{Status: &v1.KarpenterNodeStatusPoll{}},
		}); err != nil {
			return err
		}
		done, ferr := recvDeleteStatus(stream)
		if ferr != nil {
			return ferr
		}
		if done {
			return nil
		}
		if err := waitOrCtxDone(ctx, karpenterNodePollInterval); err != nil {
			return err
		}
	}
}

func recvAcceptedOperationID(stream v1.KarpenterNodeService_StreamOperationsClient, clientOperationID string) (string, error) {
	in, err := stream.Recv()
	if err != nil {
		return "", err
	}
	su := in.GetStatusUpdate()
	if su == nil {
		return "", fmt.Errorf("expected first message to be status update")
	}
	if su.State != v1.KARPENTER_NODE_OPERATION_STATE_ACCEPTED {
		return "", fmt.Errorf("unexpected first state: %v", su.State)
	}
	if in.OperationId != "" {
		return in.OperationId, nil
	}
	if clientOperationID != "" {
		return clientOperationID, nil
	}
	return "", fmt.Errorf("missing operation_id on accepted message")
}

func recvStatusOrAddResult(stream v1.KarpenterNodeService_StreamOperationsClient) (providerIDs []string, done bool, err error) {
	in, err := stream.Recv()
	if err != nil {
		return nil, false, err
	}
	switch p := in.GetPayload().(type) {
	case *v1.KarpenterNodeStreamBrokerToClient_StatusUpdate:
		if p.StatusUpdate == nil {
			return nil, false, fmt.Errorf("empty status update")
		}
		switch p.StatusUpdate.State {
		case v1.KARPENTER_NODE_OPERATION_STATE_FAILED:
			return nil, false, fmt.Errorf("karpenter op failed: %s", p.StatusUpdate.Detail)
		case v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED:
			in2, err := stream.Recv()
			if err != nil {
				return nil, false, err
			}
			if ar := in2.GetAddResult(); ar != nil {
				return ar.ProviderIds, true, nil
			}
			if in2.GetDeleteResult() != nil {
				return nil, true, nil
			}
			return nil, true, nil
		default:
			return nil, false, nil
		}
	case *v1.KarpenterNodeStreamBrokerToClient_AddResult:
		return p.AddResult.GetProviderIds(), true, nil
	default:
		return nil, false, fmt.Errorf("unexpected broker message type")
	}
}

func recvDeleteStatus(stream v1.KarpenterNodeService_StreamOperationsClient) (done bool, err error) {
	in, err := stream.Recv()
	if err != nil {
		return false, err
	}
	switch p := in.GetPayload().(type) {
	case *v1.KarpenterNodeStreamBrokerToClient_StatusUpdate:
		if p.StatusUpdate == nil {
			return false, fmt.Errorf("empty status update")
		}
		switch p.StatusUpdate.State {
		case v1.KARPENTER_NODE_OPERATION_STATE_FAILED:
			return false, fmt.Errorf("karpenter op failed: %s", p.StatusUpdate.Detail)
		case v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED:
			in2, err := stream.Recv()
			if err != nil {
				return false, err
			}
			if in2.GetDeleteResult() != nil {
				return true, nil
			}
			return true, nil
		default:
			return false, nil
		}
	case *v1.KarpenterNodeStreamBrokerToClient_DeleteResult:
		return true, nil
	default:
		return false, fmt.Errorf("unexpected broker message type")
	}
}
