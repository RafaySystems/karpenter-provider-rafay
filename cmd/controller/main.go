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

package main

import (
	"os"
	"strconv"
	"strings"

	_ "github.com/RafaySystems/karpenter-provider-rafay/pkg/operator"

	"github.com/awslabs/operatorpkg/controller"
	"k8s.io/klog/v2"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/broker"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	rafaynodeclassctrl "github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/rafaynodeclass"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/metrics"
	corecontrollers "sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	coreoperator "sigs.k8s.io/karpenter/pkg/operator"
)

const defaultEdgeClientServerPort = 5448

func main() {
	ctx, op := coreoperator.NewOperator()

	clusterID := os.Getenv("RAFAY_CLUSTER_ID")
	projectID := os.Getenv("RAFAY_PROJECT_ID")

	certFolder := strings.TrimSpace(os.Getenv("CERT_FOLDER"))
	if certFolder == "" {
		certFolder = strings.TrimSpace(os.Getenv("EDGE_CLIENT_CERT_FOLDER"))
	}

	// Default false: same secured dial as edge-client (GetEdgeClientCredentials + TLS to client.crt OU + EDGE_CLIENT_SERVER_PORT).
	// Set EDGE_BROKER_GRPC_INSECURE=true only for a plaintext broker listener (e.g. rcloud internal :5449).
	grpcInsecure := false
	if v, ok := os.LookupEnv("EDGE_BROKER_GRPC_INSECURE"); ok {
		grpcInsecure = strings.EqualFold(strings.TrimSpace(v), "true")
	}
	dialHost := strings.TrimSpace(os.Getenv("EDGE_BROKER_GRPC_HOST"))

	if !grpcInsecure || dialHost == "" {
		if certFolder == "" {
			klog.Exitf("set CERT_FOLDER or EDGE_CLIENT_CERT_FOLDER (same TLS material as edge-client), or set EDGE_BROKER_GRPC_INSECURE=true with EDGE_BROKER_GRPC_HOST")
		}
	}

	var certPath, keyPath, caPath string
	if certFolder != "" {
		certPath = certFolder + "/client.crt"
		keyPath = certFolder + "/client.key"
		caPath = certFolder + "/ca.crt"
	}

	port := 0
	if grpcInsecure {
		if p := strings.TrimSpace(os.Getenv("EDGE_BROKER_GRPC_PORT")); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				port = v
			}
		}
	} else {
		port = defaultEdgeClientServerPort
		if p := strings.TrimSpace(os.Getenv("SERVER_PORT")); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				port = v
			}
		} else if p := strings.TrimSpace(os.Getenv("EDGE_CLIENT_SERVER_PORT")); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				port = v
			}
		}
	}

	// Same sources as edge-client (see edge-client/main.go + TLS material):
	// - Broker host: client.crt OU (dial) — already handled in broker dial.
	// - Port: EDGE_CLIENT_SERVER_PORT / SERVER_PORT — above.
	// - Session id: edge-client uses common.NewID() (UUID v4) per process; optional STREAM_ID override.
	// - Edge id to broker: TLS client cert Subject Organization (O), not from env — optional EDGE_ID for logging only.
	edgeID := strings.TrimSpace(os.Getenv("EDGE_ID"))
	streamID := strings.TrimSpace(os.Getenv("STREAM_ID"))
	if streamID == "" {
		streamID = broker.NewSessionID()
		klog.Infof("STREAM_ID unset; generated session id %s (edge-client uses common.NewID() per run). If the broker must match edge-client’s stream, set STREAM_ID to the id from edge-client logs (\"using session\")", streamID)
	}
	if !grpcInsecure && certPath != "" {
		certO, err := broker.GetEdgeIDFromClientCert(certPath)
		if err != nil {
			klog.Exitf("edge-broker reads client id from TLS cert Subject Organization (O): %v", err)
		}
		if edgeID == "" {
			edgeID = certO
		} else if edgeID != certO {
			klog.Warningf("EDGE_ID=%q differs from client.crt Organization (O)=%q; the broker uses O from the mTLS certificate", edgeID, certO)
		}
	}
	if grpcInsecure {
		klog.Warning("EDGE_BROKER_GRPC_INSECURE=true: edge-broker typically requires mTLS so it can read the client certificate (NO CLIENT ID if the peer cert is missing)")
	}
	rafayClient := rafay.NewBrokerClient(certPath, keyPath, caPath, port, edgeID, streamID, dialHost, grpcInsecure)

	cp := cloudprovider.NewCloudProvider(op.GetClient(), rafayClient, clusterID, projectID)
	cloudProvider := metrics.Decorate(cp)
	clusterState := state.NewCluster(op.Clock, op.GetClient(), cloudProvider)

	coreCtrls := corecontrollers.NewControllers(
		ctx,
		op.Manager,
		op.Clock,
		op.GetClient(),
		op.EventRecorder,
		cloudProvider,
		clusterState,
	)
	allCtrls := make([]controller.Controller, 0, 1+len(coreCtrls))
	allCtrls = append(allCtrls, rafaynodeclassctrl.NewController(op.GetClient()))
	allCtrls = append(allCtrls, coreCtrls...)

	op.WithControllers(ctx, allCtrls...).Start(ctx)
}
