#!/usr/bin/env bash
# Run Karpenter Rafay provider locally against current kubeconfig cluster (edge-broker only).
# Requires TLS material and broker session env (same as edge-client).

set -e
cd "$(dirname "$0")/.."

export KARPENTER_DISABLE_LEADER_ELECTION=true

: "${EDGE_CLIENT_CERT_FOLDER:=${CERT_FOLDER:-}}"
if [ -z "$EDGE_CLIENT_CERT_FOLDER" ]; then
	echo "Set EDGE_CLIENT_CERT_FOLDER or CERT_FOLDER to a directory containing client.crt, client.key, ca.crt" >&2
	exit 1
fi
export EDGE_CLIENT_CERT_FOLDER

export RAFAY_CLUSTER_ID="${RAFAY_CLUSTER_ID:-local-dev}"
export RAFAY_PROJECT_ID="${RAFAY_PROJECT_ID:-}"
export EDGE_CLIENT_SERVER_PORT="${EDGE_CLIENT_SERVER_PORT:-5448}"
export EDGE_ID="${EDGE_ID:-}"
export STREAM_ID="${STREAM_ID:-}"

echo "Using KUBECONFIG=${KUBECONFIG:-$HOME/.kube/config}"
echo "Broker: CERT=${EDGE_CLIENT_CERT_FOLDER} PORT=${EDGE_CLIENT_SERVER_PORT} CLUSTER_ID=${RAFAY_CLUSTER_ID} EDGE_ID=${EDGE_ID:-'(empty)'} STREAM_ID=${STREAM_ID:-'(empty)'}"
echo ""

go run ./cmd/controller
