#!/bin/bash
# Example: Run client with custom settings

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

# Verify we're in the right directory
if [ ! -f "go.mod" ]; then
    echo "Error: go.mod not found. Please run this script from the project directory."
    exit 1
fi

export CLUSTER_MODE=client
export MASTER_ADDR=192.168.1.100
export MASTER_PORT=9999
export CLUSTER_TLS=true
export CLUSTER_CA_FILE=/path/to/ca.pem
export UDP_ADDR=:516
export HTTP_ADDR=:9794
export SPOOL_DIR=/tmp/udp-logger-spool

go run ./cmd/udp-logger

