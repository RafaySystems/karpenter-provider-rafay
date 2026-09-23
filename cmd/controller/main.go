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
	"time"

	_ "github.com/RafaySystems/karpenter-provider-rafay/pkg/operator"

	"github.com/awslabs/operatorpkg/controller"
	"k8s.io/klog/v2"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/broker"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	batchresumectrl "github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/batchresume"
	headroomctrl "github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/headroom"
	nodeadoptionctrl "github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/nodeadoption"
	nodeconfigctrl "github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/nodeconfig"
	nodeprovideridctrl "github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/nodeproviderid"
	rafaynodeclassctrl "github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/rafaynodeclass"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/metrics"
	corecontrollers "sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	coreoperator "sigs.k8s.io/karpenter/pkg/operator"
)

const defaultEdgeClientServerPort = 5448

// envPortOrDefault parses the named environment variable as a port number. Unset returns def;
// an invalid value logs a warning and keeps the default (instead of silently ignoring it).
func envPortOrDefault(name string, def int) int {
	p := strings.TrimSpace(os.Getenv(name))
	if p == "" {
		return def
	}
	v, err := strconv.Atoi(p)
	if err != nil {
		klog.Warningf("invalid %s=%q, using default %d", name, p, def)
		return def
	}
	return v
}

// envDurationOrDefault parses the named environment variable as a Go duration ("5m", "90s").
// Unset returns def; an invalid value logs a warning and keeps the default.
func envDurationOrDefault(name string, def time.Duration) time.Duration {
	p := strings.TrimSpace(os.Getenv(name))
	if p == "" {
		return def
	}
	v, err := time.ParseDuration(p)
	if err != nil {
		klog.Warningf("invalid %s=%q, using default %s", name, p, def)
		return def
	}
	return v
}

func main() {
	ctx, op := coreoperator.NewOperator()

	clusterID := os.Getenv("RAFAY_CLUSTER_ID")
	projectID := os.Getenv("RAFAY_PROJECT_ID")
	if clusterID == "" {
		klog.Warning("RAFAY_CLUSTER_ID is not set; node provisioning will fail — set RAFAY_CLUSTER_ID on the controller (RafayNodeClass has no per-class override)")
	}
	if projectID == "" {
		klog.Warning("RAFAY_PROJECT_ID is not set; node provisioning may fail — set RAFAY_PROJECT_ID on the controller (RafayNodeClass has no per-class override)")
	}

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
		port = envPortOrDefault("EDGE_BROKER_GRPC_PORT", 0)
	} else {
		port = defaultEdgeClientServerPort
		if strings.TrimSpace(os.Getenv("SERVER_PORT")) != "" {
			port = envPortOrDefault("SERVER_PORT", defaultEdgeClientServerPort)
		} else if strings.TrimSpace(os.Getenv("EDGE_CLIENT_SERVER_PORT")) != "" {
			port = envPortOrDefault("EDGE_CLIENT_SERVER_PORT", defaultEdgeClientServerPort)
		}
	}

	// Same sources as edge-client (see edge-client/main.go + TLS material):
	// - Broker host: client.crt OU (dial) — already handled in broker dial.
	// - Port: EDGE_CLIENT_SERVER_PORT / SERVER_PORT — above.
	// - Session id: edge-client uses common.NewID() (UUID v4) per process; optional STREAM_ID override.
	// - Edge id for logging: first DNS label of TLS client cert Subject O (same as strings.Split(O,".")[0]); optional EDGE_ID override (normalized the same way).
	edgeID := strings.TrimSpace(os.Getenv("EDGE_ID"))
	streamID := strings.TrimSpace(os.Getenv("STREAM_ID"))
	if streamID == "" {
		streamID = broker.NewSessionID()
		klog.Infof("STREAM_ID unset; generated session id %s (edge-client uses common.NewID() per run). If the broker must match edge-client’s stream, set STREAM_ID to the id from edge-client logs (\"using session\")", streamID)
	}
	if !grpcInsecure && certPath != "" {
		certHash, err := broker.GetEdgeIDFromClientCert(certPath)
		if err != nil {
			klog.Exitf("edge-broker reads client id from TLS cert Subject Organization (O): %v", err)
		}
		if edgeID == "" {
			edgeID = certHash
		} else {
			envHash := broker.EdgeHashIDFromOrganization(edgeID)
			if envHash == "" {
				klog.Warningf("EDGE_ID=%q normalizes to empty edge hash; using cert hash %q", edgeID, certHash)
				edgeID = certHash
			} else {
				if envHash != certHash {
					klog.Warningf("EDGE_ID=%q (hash %q) differs from client.crt O hash %q; the broker uses the full O from the mTLS peer certificate", edgeID, envHash, certHash)
				}
				edgeID = envHash
			}
		}
	}
	if grpcInsecure {
		klog.Warning("EDGE_BROKER_GRPC_INSECURE=true: edge-broker typically requires mTLS so it can read the client certificate (NO CLIENT ID if the peer cert is missing)")
	}
	rafayClient := rafay.NewBrokerClient(certPath, keyPath, caPath, port, edgeID, streamID, dialHost, grpcInsecure)
	// Pools the broker reports at their platform maximum are held back from provisioning for
	// this long (0 disables the hold). Shared between the failure handler, which marks pools,
	// and the cloud provider, which withholds their offerings.
	poolBackoff := cloudprovider.NewPoolBackoff(envDurationOrDefault("RAFAY_POOL_AT_MAX_COOLDOWN", cloudprovider.DefaultPoolAtMaxCooldown))
	// FAILED add operations delete the matching pending NodeClaim so Karpenter reprovisions
	// immediately (a "pool at maximum" refusal also holds the pool back first); must be
	// registered before the batcher starts polling.
	rafayClient.Batcher().SetFailureHandler(cloudprovider.NewBatchFailureHandler(op.GetClient(), op.Manager.GetAPIReader(), poolBackoff))
	// Start the batch sender + status poller goroutines; they run until ctx is cancelled.
	rafayClient.StartBatcher(ctx)

	cp := cloudprovider.NewCloudProvider(op.GetClient(), op.Manager.GetAPIReader(), rafayClient, clusterID, projectID, rafayClient.Batcher(), poolBackoff)
	cloudProvider := metrics.Decorate(cp)
	clusterState := state.NewCluster(op.Clock, op.GetClient(), cloudProvider)

	coreCtrls := corecontrollers.NewControllers(
		ctx,
		op.Manager,
		op.Clock,
		op.GetClient(),
		op.EventRecorder,
		cloudProvider,
		cp,
		clusterState,
		op.InstanceTypeStore,
	)
	// Headroom pods run in HEADROOM_NAMESPACE; the policy ConfigMap is read from
	// HEADROOM_CONFIG_NAMESPACE (defaults to the pod namespace).
	headroomNamespace := strings.TrimSpace(os.Getenv("HEADROOM_NAMESPACE"))
	if headroomNamespace == "" {
		headroomNamespace = "karpenter"
	}
	headroomConfigNamespace := strings.TrimSpace(os.Getenv("HEADROOM_CONFIG_NAMESPACE"))
	if headroomConfigNamespace == "" {
		headroomConfigNamespace = headroomNamespace
	}

	allCtrls := make([]controller.Controller, 0, 5+len(coreCtrls))
	allCtrls = append(allCtrls, rafaynodeclassctrl.NewController(op.GetClient()))
	allCtrls = append(allCtrls, nodeprovideridctrl.NewController(op.GetClient()))
	allCtrls = append(allCtrls, headroomctrl.NewController(headroomNamespace, headroomConfigNamespace))
	// Give the worker nodes the platform created before Karpenter ran a NodeClaim each, so a pool's
	// existing nodes count towards its limits and can be consolidated — without them Karpenter only
	// ever manages the nodes it added itself.
	if nodeadoptionctrl.EnabledFromEnv() {
		allCtrls = append(allCtrls, nodeadoptionctrl.NewController(op.GetClient(), cp))
	} else {
		klog.Info("KARPENTER_ADOPT_EXISTING_NODES=false: not creating NodeClaims for pre-existing worker nodes; Karpenter will not account for or consolidate them")
	}
	// Pull this cluster's node pools and node SKUs from edge-broker and apply them as
	// RafayNodeClass + NodePool, so a cluster does not need hand-written manifests that
	// duplicate (and drift from) the platform's worker-node catalog.
	if nodeconfigctrl.EnabledFromEnv() {
		allCtrls = append(allCtrls, nodeconfigctrl.NewController(rafayClient, clusterID, projectID, nodeconfigctrl.SyncIntervalFromEnv()))
	} else {
		klog.Info("KARPENTER_CONFIG_BOOTSTRAP=false: not syncing RafayNodeClass/NodePool from edge-broker; apply them yourself")
	}
	// Resume polling the add batches a previous incarnation of this provider sent. Without it a
	// restart trades the batcher's ~2-minute failure signal for Karpenter's 60-minute
	// registration timeout on every add that was in flight.
	allCtrls = append(allCtrls, batchresumectrl.NewController(op.Manager.GetAPIReader(), rafayClient.Batcher()))
	allCtrls = append(allCtrls, coreCtrls...)

	op.WithControllers(ctx, allCtrls...).Start(ctx)
}
