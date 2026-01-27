#!/bin/bash
# Build and install script for UDP Logger Client

set -e

PROJECT_DIR="/root/rabbit-log-writer"
BINARY_NAME="udp-logger"
SERVICE_NAME="udp-logger-client"

echo "UDP Logger Client - Build and Install"
echo "======================================"
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Go is not installed. Please choose an option:"
    echo ""
    echo "Option 1: Install Go on this server"
    echo "  sudo apt install golang-go"
    echo "  or"
    echo "  sudo snap install go"
    echo ""
    echo "Option 2: Build on another machine and copy the binary"
    echo "  On a machine with Go:"
    echo "    cd /path/to/rabbit-log-writer"
    echo "    go mod tidy"
    echo "    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags=\"-s -w\" -o udp-logger ./cmd/udp-logger"
    echo "    scp udp-logger user@server:/root/rabbit-log-writer/"
    echo ""
    exit 1
fi

# Check project directory
if [ ! -d "$PROJECT_DIR" ]; then
    echo "Error: Project directory $PROJECT_DIR not found"
    exit 1
fi

cd "$PROJECT_DIR"

# Ensure go.sum exists
if [ ! -f "go.sum" ]; then
    echo "go.sum not found, running go mod tidy..."
    go mod tidy
fi

# Build binary
echo "Building binary..."
go build -trimpath -ldflags="-s -w" -o "$BINARY_NAME" ./cmd/udp-logger

if [ ! -f "$BINARY_NAME" ]; then
    echo "Error: Build failed"
    exit 1
fi

echo "Binary built successfully: $BINARY_NAME"
ls -lh "$BINARY_NAME"

# Make executable
chmod +x "$BINARY_NAME"

# Install systemd service
echo ""
echo "Installing systemd service..."

if [ -f "./scripts/udp-logger-client.service" ]; then
    sudo cp "./scripts/udp-logger-client.service" "/etc/systemd/system/${SERVICE_NAME}.service"
    echo "Service file installed"
else
    echo "Error: scripts/udp-logger-client.service not found"
    exit 1
fi

# Ensure environment file exists
if [ ! -f "./scripts/client.env" ]; then
    if [ -f "./scripts/client.env.example" ]; then
        cp "./scripts/client.env.example" "./scripts/client.env"
        echo "Created client.env from example - please edit it!"
    else
        echo "Warning: No environment file found"
    fi
fi

# Reload systemd
sudo systemctl daemon-reload

echo ""
echo "======================================"
echo "Installation complete!"
echo ""
echo "Next steps:"
echo "1. Edit environment file if needed:"
echo "   nano $PROJECT_DIR/scripts/client.env"
echo ""
echo "2. Ensure CA certificate is at the path specified in CLUSTER_CA_FILE"
echo ""
echo "3. Start the service:"
echo "   sudo systemctl start $SERVICE_NAME"
echo ""
echo "4. Enable on boot:"
echo "   sudo systemctl enable $SERVICE_NAME"
echo ""
echo "5. Check status:"
echo "   sudo systemctl status $SERVICE_NAME"
echo ""
echo "6. View logs:"
echo "   sudo journalctl -u $SERVICE_NAME -f"

