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
	maxMessageSize = 20 * 1024 * 1024 // 20MB, same as edge-client
)

// NewSecureGrpcClientConn mirrors edge-client/pkg/util.NewSecureGrpcClientConn (same dial options).
func NewSecureGrpcClientConn(ctx context.Context, host string, port int, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	return Dial(ctx, host, port, creds)
}

// NewInsecureGrpcClientConn dials without TLS (rcloud internal broker listener, typically :5449).
func NewInsecureGrpcClientConn(_ context.Context, host string, port int) (*grpc.ClientConn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Minute,
			Timeout:             30 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessageSize)),
	)
}

// Dial opens a gRPC connection to the edge-broker (same options as edge-client).
// The connection is established lazily; BrokerClient reuses it across calls via getConn.
func Dial(_ context.Context, host string, port int, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Minute,
			Timeout:             30 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessageSize)),
	)
}
