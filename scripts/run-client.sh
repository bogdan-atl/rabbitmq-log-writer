#!/bin/bash
# Quick start script for Client mode using go run

set -e

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

# Verify we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "Error: go.mod not found. Please run this script from the project directory."
    echo "Current directory: $(pwd)"
    exit 1
fi

# Default values
MASTER_ADDR="${MASTER_ADDR:-master.example.com}"
MASTER_PORT="${MASTER_PORT:-9999}"
CLUSTER_CA_FILE="${CLUSTER_CA_FILE:-./certs/ca.pem}"
UDP_ADDR="${UDP_ADDR:-:516}"
SPOOL_DIR="${SPOOL_DIR:-/tmp/udp-logger-spool}"

echo "Starting UDP Logger Client mode..."
echo "Project root: $PROJECT_ROOT"
echo "Master: $MASTER_ADDR:$MASTER_PORT"
echo "UDP: $UDP_ADDR"
echo "Spool: $SPOOL_DIR"
echo ""

# Export environment variables
export CLUSTER_MODE=client
export MASTER_ADDR
export MASTER_PORT
export CLUSTER_TLS=true
export CLUSTER_CA_FILE
export UDP_ADDR
export HTTP_ADDR=:9794
export SPOOL_DIR
export SPOOL_MAX_BYTES=1073741824
export SPOOL_SEGMENT_BYTES=16777216
export SPOOL_FSYNC=false
export SPOOL_LOG_INTERVAL=30s
export BUFFER_SIZE=1000
export UDP_READ_BUFFER=1024
export PUBLISH_RETRY_INTERVAL=5s

# Run
go run ./cmd/udp-logger

