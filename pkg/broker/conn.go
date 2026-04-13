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

package broker

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultDialTimeout = 30 * time.Second
	maxMessageSize     = 20 * 1024 * 1024 // 20MB, same as edge-client
)

// NewSecureGrpcClientConn mirrors edge-client/pkg/util.NewSecureGrpcClientConn (same dial options).
func NewSecureGrpcClientConn(ctx context.Context, host string, port int, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	return Dial(ctx, host, port, creds)
}

// NewInsecureGrpcClientConn dials without TLS (rcloud internal broker listener, typically :5449).
func NewInsecureGrpcClientConn(ctx context.Context, host string, port int) (*grpc.ClientConn, error) {
	nctx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer cancel()
	addr := fmt.Sprintf("%s:%d", host, port)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessageSize)),
	}
	return grpc.DialContext(nctx, addr, opts...)
}

// Dial opens a gRPC connection to the edge-broker (same options as edge-client).
// Connection is not kept open; caller must defer conn.Close().
func Dial(ctx context.Context, host string, port int, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	nctx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer cancel()
	addr := fmt.Sprintf("%s:%d", host, port)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessageSize)),
	}
	return grpc.DialContext(nctx, addr, opts...)
}
