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

// rf_brokerclient_test.go — tests for BrokerClient: the pure helpers (shouldRedial,
// streamContext), dialBroker's host/port resolution, the retry policy of callBroker /
// callBrokerOnce (one shared conn that is never closed on an RPC failure), and the public RPC
// wrappers against the in-process broker from rf_batch_stream_test.go.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// rfNewClient returns a BrokerClient pointed (insecurely) at the loopback broker on port, with
// the given stream id. Its connection is closed when the test ends.
func rfNewClient(t *testing.T, port int, streamID string) *BrokerClient {
	t.Helper()
	c := NewBrokerClient("", "", "", port, "edge-1", streamID, "127.0.0.1", true)
	t.Cleanup(c.closeConn)
	return c
}

// ──────────────────────────── pure helpers ─────────────────────────────────

func TestRfShouldRedial(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "Unavailable", err: status.Error(codes.Unavailable, "transport closing"), want: true},
		{name: "Unavailable wrapped once", err: fmt.Errorf("sendBatch: recv: %w", status.Error(codes.Unavailable, "x")), want: true},
		{name: "Unavailable wrapped twice", err: fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", status.Error(codes.Unavailable, "x"))), want: true},
		{name: "Canceled", err: status.Error(codes.Canceled, "grpc: the client connection is closing"), want: false},
		{name: "DeadlineExceeded", err: status.Error(codes.DeadlineExceeded, "x"), want: false},
		{name: "Internal", err: status.Error(codes.Internal, "x"), want: false},
		{name: "plain error", err: errors.New("dial tcp: connection refused"), want: false},
		{name: "ErrBatchRejected", err: fmt.Errorf("sendBatch: %w: queue full", ErrBatchRejected), want: false},
		{name: "context.Canceled", err: context.Canceled, want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRedial(tc.err); got != tc.want {
				t.Errorf("shouldRedial(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestRfStreamContext(t *testing.T) {
	tests := []struct {
		name     string
		streamID string
		want     string // expected sessionid metadata; "" means no metadata at all
	}{
		{name: "stream id set", streamID: "sess-1", want: "sess-1"},
		{name: "stream id trimmed", streamID: "  sess-2\t", want: "sess-2"},
		{name: "blank stream id adds nothing", streamID: "   ", want: ""},
		{name: "empty stream id adds nothing", streamID: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &BrokerClient{StreamID: tc.streamID}
			ctx := c.streamContext(context.Background())
			md, ok := metadata.FromOutgoingContext(ctx)
			if tc.want == "" {
				if ok && len(md.Get(grpcMetadataSessionID)) > 0 {
					t.Errorf("sessionid metadata = %v, want none for blank stream id", md.Get(grpcMetadataSessionID))
				}
				return
			}
			if !ok {
				t.Fatal("no outgoing metadata on the stream context")
			}
			if got := md.Get(grpcMetadataSessionID); len(got) != 1 || got[0] != tc.want {
				t.Errorf("sessionid metadata = %v, want [%s]", got, tc.want)
			}
		})
	}
}

// ──────────────────────────── dialBroker ───────────────────────────────────

// rfWriteSelfSignedCert writes a self-signed client certificate (and its key) whose Subject OU
// carries the broker host and whose Subject O carries the edge id, exactly like the edge-client
// certificate the provider mounts. Returns cert, key and CA paths (the CA is the cert itself).
func rfWriteSelfSignedCert(t *testing.T, ou, org string) (certPath, keyPath, caPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{OrganizationalUnit: []string{ou}, Organization: []string{org}, CommonName: "client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "client.crt")
	keyPath = filepath.Join(dir, "client.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath, certPath
}

// TestRfDialBrokerTargetResolution pins how dialBroker picks host and port. grpc.NewClient is
// lazy, so no listener is needed: the resolved target is read back from the ClientConn.
func TestRfDialBrokerTargetResolution(t *testing.T) {
	certPath, keyPath, caPath := rfWriteSelfSignedCert(t, "broker.example.com", "edgeid.cluster.example.com")
	tests := []struct {
		name       string
		client     *BrokerClient
		wantTarget string // host:port the conn was created for
		wantErr    string
	}{
		{
			name:       "insecure with explicit host and port",
			client:     &BrokerClient{DialHost: "127.0.0.1", BrokerPort: 4242, GRPCInsecure: true},
			wantTarget: "127.0.0.1:4242",
		},
		{
			name:       "insecure defaults to the broker RPC port 5449",
			client:     &BrokerClient{DialHost: " 127.0.0.1 ", GRPCInsecure: true},
			wantTarget: "127.0.0.1:5449",
		},
		{
			name:       "insecure host from the certificate OU when DialHost is empty",
			client:     &BrokerClient{CertPath: certPath, GRPCInsecure: true},
			wantTarget: "broker.example.com:5449",
		},
		{
			name:       "TLS defaults to the edge-client TLS port 5448 and the OU host",
			client:     &BrokerClient{CertPath: certPath, KeyPath: keyPath, CAPath: caPath},
			wantTarget: "broker.example.com:5448",
		},
		{
			name:       "TLS with an explicit DialHost keeps it over the OU host",
			client:     &BrokerClient{CertPath: certPath, KeyPath: keyPath, CAPath: caPath, DialHost: "127.0.0.1", BrokerPort: 9},
			wantTarget: "127.0.0.1:9",
		},
		{
			name:    "no host and no certificate is an error",
			client:  &BrokerClient{GRPCInsecure: true},
			wantErr: "broker dial host",
		},
		{
			name:    "no host and unreadable certificate is an error",
			client:  &BrokerClient{CertPath: filepath.Join(t.TempDir(), "missing.crt"), GRPCInsecure: true},
			wantErr: "broker dial host",
		},
		{
			name:    "TLS with unreadable key is a credentials error",
			client:  &BrokerClient{CertPath: certPath, KeyPath: filepath.Join(t.TempDir(), "missing.key"), CAPath: caPath},
			wantErr: "broker credentials",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := tc.client.dialBroker(context.Background())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("dialBroker: %v", err)
			}
			defer conn.Close()
			// Target() reports the canonical target; grpc prefixes the default dns scheme.
			if got := conn.Target(); !strings.HasSuffix(got, tc.wantTarget) {
				t.Errorf("conn target = %q, want it to end with %q", got, tc.wantTarget)
			}
		})
	}
}

// ──────────────────────────── retry policy ─────────────────────────────────

// rfConnOf records the connections a callBroker fn was invoked with.
type rfConnOf struct{ conns []*grpc.ClientConn }

func (r *rfConnOf) fn(errs ...error) func(cc *grpc.ClientConn) error {
	return func(cc *grpc.ClientConn) error {
		r.conns = append(r.conns, cc)
		if n := len(r.conns) - 1; n < len(errs) {
			return errs[n]
		}
		return nil
	}
}

// TestRfCallBrokerRetryPolicy pins the retry contract: callBroker retries exactly once, only on
// Unavailable, and on the SAME connection; callBrokerOnce never retries. Neither ever drops the
// shared conn — closing it would abort every other goroutine's in-flight stream (see
// TestRfCallBrokerRedialDoesNotAbortConcurrentStream).
func TestRfCallBrokerRetryPolicy(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "transport is closing")
	canceled := status.Error(codes.Canceled, "grpc: the client connection is closing")
	tests := []struct {
		name      string
		once      bool // use callBrokerOnce instead of callBroker
		errs      []error
		wantCalls int
		wantErr   error // expected returned error (nil for success)
	}{
		{name: "success needs one call", errs: nil, wantCalls: 1},
		{name: "Unavailable retries once and succeeds", errs: []error{unavailable, nil}, wantCalls: 2},
		{name: "Unavailable twice returns the second error", errs: []error{unavailable, unavailable}, wantCalls: 2, wantErr: unavailable},
		{name: "Canceled is not retried", errs: []error{canceled}, wantCalls: 1, wantErr: canceled},
		{name: "plain error is not retried", errs: []error{errors.New("boom")}, wantCalls: 1, wantErr: errors.New("boom")},
		{name: "once: Unavailable is not retried", once: true, errs: []error{unavailable}, wantCalls: 1, wantErr: unavailable},
		{name: "once: Canceled is not retried", once: true, errs: []error{canceled}, wantCalls: 1, wantErr: canceled},
		{name: "once: success", once: true, errs: nil, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A lazy conn to a port nobody listens on: fn never uses it, only its identity matters.
			c := rfNewClient(t, 1, "sess")
			rec := &rfConnOf{}
			var err error
			if tc.once {
				err = c.callBrokerOnce(context.Background(), rec.fn(tc.errs...))
			} else {
				err = c.callBroker(context.Background(), rec.fn(tc.errs...))
			}
			if len(rec.conns) != tc.wantCalls {
				t.Fatalf("fn invoked %d time(s), want %d", len(rec.conns), tc.wantCalls)
			}
			switch {
			case tc.wantErr == nil && err != nil:
				t.Errorf("err = %v, want nil", err)
			case tc.wantErr != nil && (err == nil || err.Error() != tc.wantErr.Error()):
				t.Errorf("err = %v, want %v", err, tc.wantErr)
			}
			// Every invocation used the one shared conn, and it is still cached afterwards: an RPC
			// failure never closes it (gRPC reconnects by itself; closing would abort other
			// goroutines' streams).
			c.mu.Lock()
			kept := c.conn
			c.mu.Unlock()
			if kept == nil {
				t.Fatal("c.conn dropped after the call; an RPC failure must never close the shared conn")
			}
			for i, cc := range rec.conns {
				if cc != kept {
					t.Errorf("invocation %d used a different conn than the cached one", i)
				}
			}
		})
	}
}

// TestRfGetConnReusesUntilClosed pins the one-long-lived-conn contract: getConn dials once and
// hands out the same conn until closeConn, after which the next getConn dials afresh.
func TestRfGetConnReusesUntilClosed(t *testing.T) {
	c := rfNewClient(t, 1, "sess")
	ctx := context.Background()
	c1, err := c.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn: %v", err)
	}
	c1again, err := c.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn: %v", err)
	}
	if c1 != c1again {
		t.Error("second getConn returned a different conn, want the cached one")
	}
	c.closeConn()
	c.closeConn() // idempotent on a nil conn
	c2, err := c.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn after close: %v", err)
	}
	if c2 == c1 {
		t.Error("getConn after closeConn returned the closed conn")
	}
	// The dial error surfaces from getConn and leaves no conn cached.
	bad := &BrokerClient{GRPCInsecure: true}
	if _, err := bad.getConn(ctx); err == nil {
		t.Error("getConn with no host and no cert succeeded, want error")
	}
	if bad.conn != nil {
		t.Error("a failed dial must not cache a conn")
	}
}

// ──────────────────────────── public wrappers over a real broker ───────────

// TestRfBrokerClientWiring drives every public RPC wrapper through a real BrokerClient to the
// in-process broker and checks the request that arrived, the sessionid metadata, and the
// mapped result.
func TestRfBrokerClientWiring(t *testing.T) {
	srv := &rfBatchServer{}
	srv.replyWith(func(msg *v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
		switch {
		case msg.GetBatchAdd() != nil:
			return &v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: msg.GetBatchAdd().GetBatchId()}}, nil
		case msg.GetBatchRemove() != nil:
			return &v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: msg.GetBatchRemove().GetBatchId()}}, nil
		case msg.GetStatusPoll() != nil:
			return &v1.KarpenterBatchStreamBrokerToClient{BatchStatus: &v1.KarpenterBatchStatusResponse{
				BatchId:     msg.GetStatusPoll().GetBatchId(),
				NodeResults: []*v1.KarpenterBatchNodeResult{{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_RUNNING}},
			}}, nil
		case msg.GetCancelOps() != nil:
			return &v1.KarpenterBatchStreamBrokerToClient{CancelAck: &v1.KarpenterBatchCancelAck{CancelledOperationIds: msg.GetCancelOps().GetOperationIds()[:1]}}, nil
		}
		return nil, status.Error(codes.InvalidArgument, "empty message")
	})
	var gotCfgReq *v1.KarpenterConfigRequest
	var gotCfgSession string
	cfg := &rfConfigServer{fn: func(ctx context.Context, req *v1.KarpenterConfigRequest) (*v1.KarpenterConfigResponse, error) {
		gotCfgReq, gotCfgSession = req, rfSessionID(ctx)
		return &v1.KarpenterConfigResponse{ClusterName: "edge-1", Revision: "r1", AutoScaling: true, NodePoolYaml: "kind: NodePool"}, nil
	}}
	c := rfNewClient(t, rfStartServer(t, srv, cfg), "sess-42")
	ctx := context.Background()

	addID, err := c.SendBatch(ctx, []*v1.KarpenterBatchNodeAddItem{{OperationId: "op-1"}})
	if err != nil || addID == "" {
		t.Fatalf("SendBatch = (%q, %v), want a batch id", addID, err)
	}
	remID, err := c.SendBatchRemove(ctx, []*v1.KarpenterBatchNodeRemoveItem{{OperationId: "op-2"}})
	if err != nil || remID == "" {
		t.Fatalf("SendBatchRemove = (%q, %v), want a batch id", remID, err)
	}
	if addID == remID {
		t.Error("add and remove batches got the same id; each send must generate its own")
	}
	results, err := c.PollBatchStatus(ctx, addID)
	if err != nil || len(results) != 1 || results[0].GetOperationId() != "op-1" {
		t.Fatalf("PollBatchStatus = (%+v, %v), want the one RUNNING result", results, err)
	}
	cancelled, err := c.CancelOperations(ctx, []string{"op-1", "op-2"})
	if err != nil || len(cancelled) != 1 || cancelled[0] != "op-1" {
		t.Fatalf("CancelOperations = (%v, %v), want [op-1]", cancelled, err)
	}
	resp, err := c.GetKarpenterConfig(ctx, "cluster-1", "project-1")
	if err != nil || resp == nil || resp.GetRevision() != "r1" || !resp.GetAutoScaling() || resp.GetNodePoolYaml() != "kind: NodePool" {
		t.Fatalf("GetKarpenterConfig = (%+v, %v), want the broker's response", resp, err)
	}
	if gotCfgReq == nil || gotCfgReq.GetClusterId() != "cluster-1" || gotCfgReq.GetProjectId() != "project-1" {
		t.Errorf("config request at broker = %+v, want cluster-1/project-1", gotCfgReq)
	}
	if gotCfgSession != "sess-42" {
		t.Errorf("config call sessionid = %q, want sess-42", gotCfgSession)
	}

	received, sessions := srv.snapshot()
	if len(received) != 4 {
		t.Fatalf("broker received %d stream message(s), want 4", len(received))
	}
	if received[0].GetBatchAdd() == nil || received[1].GetBatchRemove() == nil || received[2].GetStatusPoll() == nil || received[3].GetCancelOps() == nil {
		t.Errorf("broker received messages in the wrong shape/order: %+v", received)
	}
	if received[2].GetStatusPoll().GetBatchId() != addID {
		t.Errorf("status poll batch id = %q, want %q", received[2].GetStatusPoll().GetBatchId(), addID)
	}
	for i, s := range sessions {
		if s != "sess-42" {
			t.Errorf("stream %d sessionid = %q, want sess-42 (edge-broker routes on it)", i, s)
		}
	}
	// One long-lived connection served every call.
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		t.Error("no cached connection after successful calls")
	}
}

// TestRfBrokerClientRequiresStreamID pins that every wrapper refuses to run without a STREAM_ID
// and never dials.
func TestRfBrokerClientRequiresStreamID(t *testing.T) {
	c := &BrokerClient{GRPCInsecure: true} // no host either: a dial attempt would also fail, differently
	ctx := context.Background()
	calls := map[string]func() error{
		"SendBatch": func() error {
			_, err := c.SendBatch(ctx, []*v1.KarpenterBatchNodeAddItem{{OperationId: "op"}})
			return err
		},
		"SendBatchRemove": func() error {
			_, err := c.SendBatchRemove(ctx, []*v1.KarpenterBatchNodeRemoveItem{{OperationId: "op"}})
			return err
		},
		"PollBatchStatus":    func() error { _, err := c.PollBatchStatus(ctx, "b"); return err },
		"CancelOperations":   func() error { _, err := c.CancelOperations(ctx, []string{"op"}); return err },
		"GetKarpenterConfig": func() error { _, err := c.GetKarpenterConfig(ctx, "c", "p"); return err },
	}
	for name, call := range calls {
		if err := call(); err == nil || !strings.Contains(err.Error(), "STREAM_ID is required") {
			t.Errorf("%s without StreamID: err = %v, want STREAM_ID is required", name, err)
		}
	}
	if c.conn != nil {
		t.Error("a call refused for a missing STREAM_ID must not dial")
	}
}

// TestRfBrokerClientRetriesAfterBrokerRestart is the end-to-end form of the retry policy: the
// broker's first stream fails Unavailable, the client retries the poll once on the same conn and
// it succeeds; a send (callBrokerOnce) surfaces the failure without retrying, and the conn is
// kept in both cases so the next call reuses it.
func TestRfBrokerClientRetriesAfterBrokerRestart(t *testing.T) {
	srv := &rfBatchServer{}
	failNext := make(chan struct{}, 1)
	srv.replyWith(func(msg *v1.KarpenterBatchStreamClientToBroker) (*v1.KarpenterBatchStreamBrokerToClient, error) {
		select {
		case <-failNext:
			return nil, status.Error(codes.Unavailable, "broker restarting")
		default:
		}
		if msg.GetStatusPoll() != nil {
			return &v1.KarpenterBatchStreamBrokerToClient{BatchStatus: &v1.KarpenterBatchStatusResponse{}}, nil
		}
		return &v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: "b"}}, nil
	})
	c := rfNewClient(t, rfStartServer(t, srv, nil), "sess")
	ctx := context.Background()

	first, err := c.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn: %v", err)
	}

	failNext <- struct{}{}
	if _, err := c.PollBatchStatus(ctx, "batch-1"); err != nil {
		t.Fatalf("PollBatchStatus after an Unavailable stream = %v, want success on the retry", err)
	}
	c.mu.Lock()
	second := c.conn
	c.mu.Unlock()
	if second != first {
		t.Error("PollBatchStatus replaced the shared conn after Unavailable; the retry must reuse it")
	}
	if _, sessions := srv.snapshot(); len(sessions) != 2 {
		t.Errorf("broker saw %d stream(s), want 2 (failed + retried)", len(sessions))
	}

	failNext <- struct{}{}
	if _, err := c.SendBatch(ctx, []*v1.KarpenterBatchNodeAddItem{{OperationId: "op"}}); !shouldRedial(err) {
		t.Fatalf("SendBatch on an Unavailable stream = %v, want the Unavailable error surfaced (no retry)", err)
	}
	if _, sessions := srv.snapshot(); len(sessions) != 3 {
		t.Errorf("broker saw %d stream(s), want 3: a send must never be re-issued", len(sessions))
	}
	c.mu.Lock()
	afterSend := c.conn
	c.mu.Unlock()
	if afterSend != first {
		t.Error("callBrokerOnce must keep the shared conn on Unavailable (closing it would abort concurrent streams)")
	}
	// The next send reuses the conn and succeeds.
	if id, err := c.SendBatch(ctx, []*v1.KarpenterBatchNodeAddItem{{OperationId: "op"}}); err != nil || id != "b" {
		t.Errorf("SendBatch after the failed send = (%q, %v), want (b, nil)", id, err)
	}
}

// TestRfCallBrokerRedialDoesNotAbortConcurrentStream is the regression test for
// R1-concurrency-provider-1: closeConn() on Unavailable closes the one shared ClientConn under
// every other goroutine's in-flight stream. Here the batch sender has an add stream open (the
// broker has received the batch and is about to ACK) while the status poller hits Unavailable
// and retries. The sender's stream must be unaffected and still receive its batch id: the
// poller must not close the shared conn (that would tear the sender's stream down with
// codes.Canceled, which shouldRedial does not recover, failing Create() although the broker may
// already hold the batch ACCEPTED).
func TestRfCallBrokerRedialDoesNotAbortConcurrentStream(t *testing.T) {
	addOpen := make(chan struct{})    // closed once the broker has the add batch on a stream
	releaseAdd := make(chan struct{}) // closed to let the broker ACK the add batch
	failPoll := make(chan struct{}, 1)
	srv := &rfBatchServer{}
	srv.handle = func(stream v1.KarpenterBatchService_BatchStreamOperationsServer) error {
		msg, err := srv.recv(stream)
		if err != nil {
			return err
		}
		switch {
		case msg.GetBatchAdd() != nil:
			close(addOpen)
			select {
			case <-releaseAdd:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
			return stream.Send(&v1.KarpenterBatchStreamBrokerToClient{BatchAccepted: &v1.KarpenterBatchAccepted{BatchId: msg.GetBatchAdd().GetBatchId()}})
		case msg.GetStatusPoll() != nil:
			select {
			case <-failPoll:
				return status.Error(codes.Unavailable, "broker restarting")
			default:
				return stream.Send(&v1.KarpenterBatchStreamBrokerToClient{BatchStatus: &v1.KarpenterBatchStatusResponse{}})
			}
		}
		return status.Error(codes.InvalidArgument, "unexpected message")
	}
	c := rfNewClient(t, rfStartServer(t, srv, nil), "sess")
	ctx := context.Background()

	type sendResult struct {
		id  string
		err error
	}
	sent := make(chan sendResult, 1)
	go func() {
		id, err := c.SendBatch(ctx, []*v1.KarpenterBatchNodeAddItem{{OperationId: "op-1"}})
		sent <- sendResult{id, err}
	}()
	select {
	case <-addOpen:
	case <-time.After(waitTimeout):
		t.Fatal("broker never received the add batch")
	}

	// The poller's stream fails Unavailable; callBroker retries once on the shared conn.
	failPoll <- struct{}{}
	if _, err := c.PollBatchStatus(ctx, "batch-x"); err != nil {
		t.Fatalf("PollBatchStatus = %v, want success after the retry", err)
	}

	close(releaseAdd)
	select {
	case r := <-sent:
		if r.err != nil {
			t.Fatalf("SendBatch = %v, want the batch id: the poller's retry must not abort the sender's stream (code %v)", r.err, status.Code(r.err))
		}
		if r.id == "" {
			t.Error("SendBatch returned an empty batch id")
		}
	case <-time.After(waitTimeout):
		t.Fatal("SendBatch did not return")
	}
}
