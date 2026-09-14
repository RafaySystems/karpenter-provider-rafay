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

// batch_stream.go — gRPC helper functions for KarpenterBatchService.BatchStreamOperations.
//
// sendBatch:       open a stream, send KarpenterBatchNodeAddRequest, receive
//                  KarpenterBatchAccepted (or KarpenterBatchRejected → ErrBatchRejected),
//                  close stream. Returns the batch_id on success.
//
// sendBatchRemove: same as sendBatch but with KarpenterBatchNodeRemoveRequest.
//
// pollBatchStatus: open a stream, send KarpenterBatchStatusPoll, receive
//                  KarpenterBatchStatusResponse, close stream. Returns per-node results.
//
// cancelOps:       open a stream, send KarpenterBatchOperationCancel, receive
//                  KarpenterBatchCancelAck, close stream. Returns the operation IDs the
//                  broker actually cancelled (best-effort: only ops still ACCEPTED).
//
// Note on Send errors: per the gRPC contract, SendMsg on a stream whose transport has broken
// returns a bare io.EOF — the real status (e.g. codes.Unavailable) is only obtainable from
// Recv. Every helper below therefore calls Recv after a failed Send and returns that status
// instead, so shouldRedial() can recognise Unavailable and drive the redial/closeConn recovery
// path (and so callers see a meaningful error rather than "EOF").

import (
	"context"
	"fmt"

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"google.golang.org/grpc"

	"github.com/google/uuid"
)

// sendBatch opens a BatchStreamOperations stream, sends the batch, waits for ACCEPTED, and returns
// the batch_id. The stream is closed after the response is received. A KarpenterBatchRejected
// response (e.g. broker queue full) is returned as ErrBatchRejected wrapping the reason.
func sendBatch(ctx context.Context, conn *grpc.ClientConn, nodes []*v1.KarpenterBatchNodeAddItem) (string, error) {
	if len(nodes) == 0 {
		return "", fmt.Errorf("sendBatch: empty nodes list")
	}
	batchID := uuid.New().String()

	cli := v1.NewKarpenterBatchServiceClient(conn)
	stream, err := cli.BatchStreamOperations(ctx)
	if err != nil {
		return "", fmt.Errorf("sendBatch: open stream: %w", err)
	}

	if err := stream.Send(&v1.KarpenterBatchStreamClientToBroker{
		BatchAdd: &v1.KarpenterBatchNodeAddRequest{
			BatchId: batchID,
			Nodes:   nodes,
		},
	}); err != nil {
		// Send returned io.EOF; recover the real status from Recv (see note above).
		if _, rerr := stream.Recv(); rerr != nil {
			err = rerr
		}
		return "", fmt.Errorf("sendBatch: send: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("sendBatch: recv: %w", err)
	}
	if ba := resp.GetBatchAccepted(); ba != nil {
		_ = stream.CloseSend()
		return ba.GetBatchId(), nil
	}
	if br := resp.GetBatchRejected(); br != nil {
		_ = stream.CloseSend()
		return "", fmt.Errorf("sendBatch: %w: %s", ErrBatchRejected, br.GetReason())
	}
	return "", fmt.Errorf("sendBatch: unexpected broker response type")
}

// sendBatchRemove opens a BatchStreamOperations stream, sends the remove batch, waits for
// ACCEPTED, and returns the batch_id. The stream is closed after the response is received.
// A KarpenterBatchRejected response is returned as ErrBatchRejected wrapping the reason.
func sendBatchRemove(ctx context.Context, conn *grpc.ClientConn, nodes []*v1.KarpenterBatchNodeRemoveItem) (string, error) {
	if len(nodes) == 0 {
		return "", fmt.Errorf("sendBatchRemove: empty nodes list")
	}
	batchID := uuid.New().String()

	cli := v1.NewKarpenterBatchServiceClient(conn)
	stream, err := cli.BatchStreamOperations(ctx)
	if err != nil {
		return "", fmt.Errorf("sendBatchRemove: open stream: %w", err)
	}

	if err := stream.Send(&v1.KarpenterBatchStreamClientToBroker{
		BatchRemove: &v1.KarpenterBatchNodeRemoveRequest{
			BatchId: batchID,
			Nodes:   nodes,
		},
	}); err != nil {
		// Send returned io.EOF; recover the real status from Recv (see note above).
		if _, rerr := stream.Recv(); rerr != nil {
			err = rerr
		}
		return "", fmt.Errorf("sendBatchRemove: send: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("sendBatchRemove: recv: %w", err)
	}
	if ba := resp.GetBatchAccepted(); ba != nil {
		_ = stream.CloseSend()
		return ba.GetBatchId(), nil
	}
	if br := resp.GetBatchRejected(); br != nil {
		_ = stream.CloseSend()
		return "", fmt.Errorf("sendBatchRemove: %w: %s", ErrBatchRejected, br.GetReason())
	}
	return "", fmt.Errorf("sendBatchRemove: unexpected broker response type")
}

// pollBatchStatus opens a BatchStreamOperations stream, sends a status poll for batchID, and
// returns the per-node results. The stream is closed after the response is received.
// An unknown/expired batchID yields an empty result list (not an error).
func pollBatchStatus(ctx context.Context, conn *grpc.ClientConn, batchID string) ([]*v1.KarpenterBatchNodeResult, error) {
	cli := v1.NewKarpenterBatchServiceClient(conn)
	stream, err := cli.BatchStreamOperations(ctx)
	if err != nil {
		return nil, fmt.Errorf("pollBatchStatus: open stream: %w", err)
	}

	if err := stream.Send(&v1.KarpenterBatchStreamClientToBroker{
		StatusPoll: &v1.KarpenterBatchStatusPoll{BatchId: batchID},
	}); err != nil {
		// Send returned io.EOF; recover the real status from Recv (see note above).
		if _, rerr := stream.Recv(); rerr != nil {
			err = rerr
		}
		return nil, fmt.Errorf("pollBatchStatus: send: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("pollBatchStatus: recv: %w", err)
	}
	_ = stream.CloseSend()
	if bs := resp.GetBatchStatus(); bs != nil {
		return bs.GetNodeResults(), nil
	}
	return nil, fmt.Errorf("pollBatchStatus: unexpected broker response type")
}

// cancelOps opens a BatchStreamOperations stream, sends a best-effort cancel for the given
// operation IDs, and returns the IDs the broker actually transitioned to cancelled (only ops
// still ACCEPTED at the broker). The stream is closed after the response is received.
func cancelOps(ctx context.Context, conn *grpc.ClientConn, operationIDs []string) ([]string, error) {
	if len(operationIDs) == 0 {
		return nil, fmt.Errorf("cancelOps: empty operation ID list")
	}
	cli := v1.NewKarpenterBatchServiceClient(conn)
	stream, err := cli.BatchStreamOperations(ctx)
	if err != nil {
		return nil, fmt.Errorf("cancelOps: open stream: %w", err)
	}

	if err := stream.Send(&v1.KarpenterBatchStreamClientToBroker{
		CancelOps: &v1.KarpenterBatchOperationCancel{OperationIds: operationIDs},
	}); err != nil {
		// Send returned io.EOF; recover the real status from Recv (see note above).
		if _, rerr := stream.Recv(); rerr != nil {
			err = rerr
		}
		return nil, fmt.Errorf("cancelOps: send: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("cancelOps: recv: %w", err)
	}
	_ = stream.CloseSend()
	if ack := resp.GetCancelAck(); ack != nil {
		return ack.GetCancelledOperationIds(), nil
	}
	return nil, fmt.Errorf("cancelOps: unexpected broker response type")
}
