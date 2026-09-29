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

// rf_batch_stream_test.go — tests for the four BatchStreamOperations helpers in batch_stream.go
// against an in-process gRPC server on a loopback listener. The server implements the
// hand-written v1.KarpenterBatchServiceServer with a per-test scripted handler, so every
// response shape (accepted, rejected, status, cancel-ack, wrong type, status error) is exercised
// through the real codec and stream machinery.

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/broker"
)

// ──────────────────────────── in-process broker ────────────────────────────

// rfStreamHandler is one scripted BatchStreamOperations handler.
type rfStreamHandler func(stream v1.KarpenterBatchService_BatchStreamOperationsServer) error

// rfBatchServer is an in-process KarpenterBatchService whose stream handling is scripted per
// test. It records every client→broker message it receives, in order.
type rfBatchServer struct {
	handle rfStreamHandler

	mu       sync.Mutex
	received []*v1.KarpenterBatchStreamClientToBroker
	// sessionIDs records the "sessionid" metadata seen on each stream (empty when absent).
	sessionIDs []string
}

var _ v1.KarpenterBatchServiceServer = (*rfBatchServer)(nil)

func (s *rfBatchServer) BatchStreamOperations(stream v1.KarpenterBatchService_BatchStreamOperationsServer) error {
	s.mu.Lock()
	s.sessionIDs = append(s.sessionIDs, rfSessionID(stream.Context()))
	s.mu.Unlock()
	return s.handle(stream)
}

// recv records and returns the next client message.
func (s *rfBatchServer) recv(stream v1.KarpenterBatchService_BatchStreamOperationsServer) (*v1.KarpenterBatchStreamClientToBroker, error) {
	msg, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.received = append(s.received, msg)
	s.mu.Unlock()
	return msg, nil
}

func (s *rfBatchServer) snapshot() ([]*v1.KarpenterBatchStreamClientToBroker, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*v1.KarpenterBatchStreamClientToBroker(nil), s.received...), append([]string(nil), s.sessionIDs...)
}

// replyWith installs a request/response handler: receive one message, answer with fn's
// response (or fail the stream with fn's error).
func (s *rfBatchServer) replyWith(fn func(msg *v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error)) {
	s.handle = func(stream v1.KarpenterBatchService_BatchStreamOperationsServer) error {
		msg, err := s.recv(stream)
		if err != nil {
			return err
		}
		resp, err := fn(msg)
		if err != nil {
			return err
		}
		return stream.Send(resp)
	}
}

// rfSessionID returns the sessionid gRPC metadata value on an incoming stream context.
func rfSessionID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get(grpcMetadataSessionID); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// rfConfigServer is an in-process KarpenterConfigService with a scripted unary handler.
type rfConfigServer struct {
	fn func(ctx context.Context, req *v1.KarpenterConfigRequest) (*v1.KarpenterConfigResponse, error)
}

var _ v1.KarpenterConfigServiceServer = (*rfConfigServer)(nil)

func (s *rfConfigServer) GetKarpenterConfig(ctx context.Context, req *v1.KarpenterConfigRequest) (*v1.KarpenterConfigResponse, error) {
	return s.fn(ctx, req)
}

// rfStartServer serves the given services on a fresh loopback listener and returns its port.
// Either service may be nil. The server is stopped when the test ends.
func rfStartServer(t *testing.T, batch v1.KarpenterBatchServiceServer, cfg v1.KarpenterConfigServiceServer) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	if batch != nil {
		v1.RegisterKarpenterBatchServiceServer(srv, batch)
	}
	if cfg != nil {
		v1.RegisterKarpenterConfigServiceServer(srv, cfg)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	_, portStr, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse listener port: %v", err)
	}
	return port
}

// rfDial opens an insecure client connection to the loopback server on port, exactly as
// dialBroker does in insecure mode. Closed when the test ends.
func rfDial(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()
	conn, err := broker.NewInsecureGrpcClientConn(context.Background(), "127.0.0.1", port)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// rfCallCtx returns a bounded context for one helper call.
func rfCallCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	t.Cleanup(cancel)
	return ctx
}

// ──────────────────────────── sendBatch / sendBatchRemove ──────────────────

func TestRfSendBatchAccepted(t *testing.T) {
	srv := &rfBatchServer{}
	srv.replyWith(func(msg *v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
		add := msg.GetBatchAdd()
		if add == nil {
			return nil, status.Error(codes.InvalidArgument, "expected batch_add")
		}
		// Echo the client's batch id, as the real broker does.
		return &v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: add.GetBatchId()}}, nil
	})
	conn := rfDial(t, rfStartServer(t, srv, nil))

	nodes := []*v1.KarpenterBatchNodeAddItem{
		{OperationId: "op-1", ClusterId: "c1", ProjectId: "p1", InstanceType: "m5.large", NodePoolName: "np1"},
		{OperationId: "op-2", ClusterId: "c1", ProjectId: "p1", InstanceType: "m5.large", NodePoolName: "np1"},
	}
	batchID, err := sendBatch(rfCallCtx(t), conn, nodes)
	if err != nil {
		t.Fatalf("sendBatch: %v", err)
	}
	if batchID == "" {
		t.Fatal("sendBatch returned an empty batch id")
	}

	received, _ := srv.snapshot()
	if len(received) != 1 {
		t.Fatalf("broker received %d message(s), want 1", len(received))
	}
	add := received[0].GetBatchAdd()
	if add == nil {
		t.Fatalf("broker received %+v, want a batch_add", received[0])
	}
	// The batch id on the wire is the one the helper generated and returned.
	if add.GetBatchId() != batchID {
		t.Errorf("wire batch id = %q, want the returned id %q", add.GetBatchId(), batchID)
	}
	if len(add.GetNodes()) != 2 || add.GetNodes()[1].GetOperationId() != "op-2" || add.GetNodes()[0].GetInstanceType() != "m5.large" {
		t.Errorf("wire nodes = %+v, want the two items sent, intact", add.GetNodes())
	}
}

func TestRfSendBatchRemoveAccepted(t *testing.T) {
	srv := &rfBatchServer{}
	srv.replyWith(func(msg *v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
		rem := msg.GetBatchRemove()
		if rem == nil {
			return nil, status.Error(codes.InvalidArgument, "expected batch_remove")
		}
		return &v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: rem.GetBatchId()}}, nil
	})
	conn := rfDial(t, rfStartServer(t, srv, nil))

	nodes := []*v1.KarpenterBatchNodeRemoveItem{
		{OperationId: "op-rem", ClusterId: "c1", ProjectId: "p1", InstanceType: "m5.large", NodePoolName: "np1", ProviderId: "rafay://np1/m5.large/host-1"},
	}
	batchID, err := sendBatchRemove(rfCallCtx(t), conn, nodes)
	if err != nil {
		t.Fatalf("sendBatchRemove: %v", err)
	}
	received, _ := srv.snapshot()
	if len(received) != 1 || received[0].GetBatchRemove() == nil {
		t.Fatalf("broker received %+v, want one batch_remove", received)
	}
	rem := received[0].GetBatchRemove()
	if rem.GetBatchId() != batchID {
		t.Errorf("wire batch id = %q, want the returned id %q", rem.GetBatchId(), batchID)
	}
	if len(rem.GetNodes()) != 1 || rem.GetNodes()[0].GetProviderId() != "rafay://np1/m5.large/host-1" {
		t.Errorf("wire nodes = %+v, want the remove item with its provider id intact", rem.GetNodes())
	}
}

// TestRfSendBatchResponses covers the non-accepted responses for both send helpers: a
// KarpenterBatchRejected maps to ErrBatchRejected (errors.Is must survive every %w layer, since
// failBatchSend wraps it once more before Create()/Delete() inspect it) with the reason
// carried, a stream status error keeps its gRPC code (so shouldRedial can see Unavailable),
// and a response of the wrong type is an error.
func TestRfSendBatchResponses(t *testing.T) {
	type sendFn func(ctx context.Context, conn *grpc.ClientConn) (string, error)
	senders := map[string]sendFn{
		"add": func(ctx context.Context, conn *grpc.ClientConn) (string, error) {
			return sendBatch(ctx, conn, []*v1.KarpenterBatchNodeAddItem{{OperationId: "op-1"}})
		},
		"remove": func(ctx context.Context, conn *grpc.ClientConn) (string, error) {
			return sendBatchRemove(ctx, conn, []*v1.KarpenterBatchNodeRemoveItem{{OperationId: "op-1"}})
		},
	}
	tests := []struct {
		name         string
		resp         *v1.KarpenterBatchStreamBrokerToClient
		streamErr    error
		wantRejected bool
		wantContains string
		wantCode     codes.Code
	}{
		{
			name:         "rejected maps to ErrBatchRejected with reason",
			resp:         &v1.KarpenterBatchStreamBrokerToClient{BatchRejected: &v1.KarpenterBatchRejected{Reason: "queue full (64/64)"}},
			wantRejected: true,
			wantContains: "queue full (64/64)",
		},
		{
			name:         "unexpected response type is an error",
			resp:         &v1.KarpenterBatchStreamBrokerToClient{CancelAck: &v1.KarpenterBatchCancelAck{}},
			wantContains: "unexpected broker response type",
		},
		{
			name:      "stream status error keeps its code",
			streamErr: status.Error(codes.Unavailable, "broker restarting"),
			wantCode:  codes.Unavailable,
		},
	}
	for kind, send := range senders {
		for _, tc := range tests {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				srv := &rfBatchServer{}
				srv.replyWith(func(*v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
					return tc.resp, tc.streamErr
				})
				conn := rfDial(t, rfStartServer(t, srv, nil))

				batchID, err := send(rfCallCtx(t), conn)
				if err == nil {
					t.Fatalf("send returned batch id %q and no error", batchID)
				}
				if batchID != "" {
					t.Errorf("batch id = %q on error, want empty", batchID)
				}
				if errors.Is(err, ErrBatchRejected) != tc.wantRejected {
					t.Errorf("errors.Is(err, ErrBatchRejected) = %t, want %t (err: %v)", !tc.wantRejected, tc.wantRejected, err)
				}
				if tc.wantRejected {
					// One more %w layer, as failBatchSend adds, must not hide the sentinel.
					wrapped := failBatchSendError(kind, err)
					if !errors.Is(wrapped, ErrBatchRejected) {
						t.Errorf("ErrBatchRejected lost through failBatchSend-style wrapping: %v", wrapped)
					}
				}
				if tc.wantContains != "" && !strings.Contains(err.Error(), tc.wantContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantContains)
				}
				if tc.wantCode != codes.OK {
					if got := status.Code(err); got != tc.wantCode {
						t.Errorf("status.Code(err) = %v, want %v (err: %v)", got, tc.wantCode, err)
					}
					if !shouldRedial(err) {
						t.Error("shouldRedial must recognise Unavailable through the helper's wrapping")
					}
				}
			})
		}
	}
}

// failBatchSendError reproduces the wrapping failBatchSend applies to a send error.
func failBatchSendError(kind string, err error) error {
	b := newTestBatcher(&mockBroker{})
	ch := b.Enqueue("op-wrap", AddNodesRequest{})
	<-b.queue
	b.failBatchSend(kind, []batchItem{{operationID: "op-wrap"}}, err)
	return (<-ch).Err
}

// TestRfSendHelpersRefuseEmptyBatch pins that an empty node list is refused before any stream
// is opened: the broker never sees a request.
func TestRfSendHelpersRefuseEmptyBatch(t *testing.T) {
	srv := &rfBatchServer{}
	srv.replyWith(func(*v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
		return &v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: "never"}}, nil
	})
	conn := rfDial(t, rfStartServer(t, srv, nil))
	ctx := rfCallCtx(t)

	if id, err := sendBatch(ctx, conn, nil); err == nil || id != "" {
		t.Errorf("sendBatch(nil) = (%q, %v), want error", id, err)
	}
	if id, err := sendBatchRemove(ctx, conn, []*v1.KarpenterBatchNodeRemoveItem{}); err == nil || id != "" {
		t.Errorf("sendBatchRemove(empty) = (%q, %v), want error", id, err)
	}
	if ids, err := cancelOps(ctx, conn, nil); err == nil || ids != nil {
		t.Errorf("cancelOps(nil) = (%v, %v), want error", ids, err)
	}
	if received, sessions := srv.snapshot(); len(received) != 0 || len(sessions) != 0 {
		t.Errorf("broker saw %d message(s) on %d stream(s), want none for empty requests", len(received), len(sessions))
	}
}

// ──────────────────────────── pollBatchStatus ──────────────────────────────

func TestRfPollBatchStatus(t *testing.T) {
	tests := []struct {
		name        string
		resp        *v1.KarpenterBatchStreamBrokerToClient
		streamErr   error
		wantResults int
		wantErr     string
		wantCode    codes.Code
	}{
		{
			name: "results are returned per node",
			resp: &v1.KarpenterBatchStreamBrokerToClient{BatchStatus: &v1.KarpenterBatchStatusResponse{
				BatchId: "batch-1",
				NodeResults: []*v1.KarpenterBatchNodeResult{
					{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED, ProviderIds: []string{"rafay://np1/sku/h1"}},
					{OperationId: "op-2", State: v1.KARPENTER_NODE_OPERATION_STATE_FAILED, Detail: "no capacity"},
				},
			}},
			wantResults: 2,
		},
		{
			// The real broker answers an unknown batch id with an empty list, not an error.
			name:        "unknown batch yields empty results and no error",
			resp:        &v1.KarpenterBatchStreamBrokerToClient{BatchStatus: &v1.KarpenterBatchStatusResponse{BatchId: "batch-1"}},
			wantResults: 0,
		},
		{
			name:    "unexpected response type is an error",
			resp:    &v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: "batch-1"}},
			wantErr: "unexpected broker response type",
		},
		{
			name:      "stream status error keeps its code",
			streamErr: status.Error(codes.Unavailable, "broker restarting"),
			wantErr:   "pollBatchStatus: recv",
			wantCode:  codes.Unavailable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &rfBatchServer{}
			srv.replyWith(func(msg *v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
				if poll := msg.GetStatusPoll(); poll == nil || poll.GetBatchId() != "batch-1" {
					return nil, status.Errorf(codes.InvalidArgument, "expected status_poll for batch-1, got %+v", msg)
				}
				return tc.resp, tc.streamErr
			})
			conn := rfDial(t, rfStartServer(t, srv, nil))

			results, err := pollBatchStatus(rfCallCtx(t), conn, "batch-1")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				if tc.wantCode != codes.OK && status.Code(err) != tc.wantCode {
					t.Errorf("status.Code(err) = %v, want %v", status.Code(err), tc.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("pollBatchStatus: %v", err)
			}
			if len(results) != tc.wantResults {
				t.Fatalf("results = %+v, want %d", results, tc.wantResults)
			}
			if tc.wantResults == 2 {
				if results[0].GetState() != v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED || results[0].GetProviderIds()[0] != "rafay://np1/sku/h1" {
					t.Errorf("result[0] = %+v, want SUCCEEDED with its provider id", results[0])
				}
				if results[1].GetState() != v1.KARPENTER_NODE_OPERATION_STATE_FAILED || results[1].GetDetail() != "no capacity" {
					t.Errorf("result[1] = %+v, want FAILED with detail", results[1])
				}
			}
		})
	}
}

// ──────────────────────────── cancelOps ────────────────────────────────────

func TestRfCancelOps(t *testing.T) {
	tests := []struct {
		name          string
		resp          *v1.KarpenterBatchStreamBrokerToClient
		wantCancelled []string
		wantErr       string
	}{
		{
			name:          "ack lists the operations actually cancelled",
			resp:          &v1.KarpenterBatchStreamBrokerToClient{CancelAck: &v1.KarpenterBatchCancelAck{CancelledOperationIds: []string{"op-1"}}},
			wantCancelled: []string{"op-1"},
		},
		{
			name: "ack with nothing cancelled is not an error",
			resp: &v1.KarpenterBatchStreamBrokerToClient{CancelAck: &v1.KarpenterBatchCancelAck{}},
		},
		{
			name:    "unexpected response type is an error",
			resp:    &v1.KarpenterBatchStreamBrokerToClient{BatchStatus: &v1.KarpenterBatchStatusResponse{}},
			wantErr: "unexpected broker response type",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &rfBatchServer{}
			srv.replyWith(func(msg *v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
				if c := msg.GetCancelOps(); c == nil || len(c.GetOperationIds()) != 2 {
					return nil, status.Errorf(codes.InvalidArgument, "expected cancel_ops with two ids, got %+v", msg)
				}
				return tc.resp, nil
			})
			conn := rfDial(t, rfStartServer(t, srv, nil))

			cancelled, err := cancelOps(rfCallCtx(t), conn, []string{"op-1", "op-2"})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("cancelOps: %v", err)
			}
			if len(cancelled) != len(tc.wantCancelled) || (len(cancelled) > 0 && cancelled[0] != tc.wantCancelled[0]) {
				t.Errorf("cancelled = %v, want %v", cancelled, tc.wantCancelled)
			}
		})
	}
}

// TestRfHelpersHonourCallerDeadline pins that a broker that never answers does not hang the
// caller past its context deadline: the helper returns DeadlineExceeded.
func TestRfHelpersHonourCallerDeadline(t *testing.T) {
	srv := &rfBatchServer{
		handle: func(stream v1.KarpenterBatchService_BatchStreamOperationsServer) error {
			<-stream.Context().Done() // never answer; return when the client gives up
			return stream.Context().Err()
		},
	}
	conn := rfDial(t, rfStartServer(t, srv, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := pollBatchStatus(ctx, conn, "batch-1")
		done <- err
	}()
	select {
	case err := <-done:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Errorf("err = %v, want DeadlineExceeded", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("pollBatchStatus did not return after its context deadline")
	}
}
