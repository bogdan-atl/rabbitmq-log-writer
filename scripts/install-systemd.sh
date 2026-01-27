#!/bin/bash
# Installation script for UDP Logger Client systemd service

set -e

SERVICE_NAME="udp-logger-client"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
ENV_FILE="/root/rabbit-log-writer/scripts/client.env"
PROJECT_DIR="/root/rabbit-log-writer"

echo "Installing UDP Logger Client systemd service..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Check if project directory exists
if [ ! -d "$PROJECT_DIR" ]; then
    echo "Error: Project directory $PROJECT_DIR not found"
    echo "Please adjust PROJECT_DIR in this script or create the directory"
    exit 1
fi

# Check if go is installed (for go run)
if ! command -v go &> /dev/null; then
    echo "Warning: Go is not installed. You may need to compile the binary first."
    echo "Or install Go: sudo apt install golang-go"
fi

# Copy service file
echo "Installing systemd service..."
if [ -f "./scripts/udp-logger-client.service" ]; then
    cp "./scripts/udp-logger-client.service" "$SERVICE_FILE"
    echo "Service file installed to $SERVICE_FILE"
else
    echo "Error: scripts/udp-logger-client.service not found"
    exit 1
fi

# Create or update environment file
echo "Setting up environment file..."
if [ -f "./scripts/client.env" ]; then
    if [ ! -f "$ENV_FILE" ]; then
        cp "./scripts/client.env" "$ENV_FILE"
        echo "Environment file created at $ENV_FILE"
    else
        echo "Environment file $ENV_FILE already exists"
        echo "Please review and update it if needed"
    fi
else
    echo "Warning: scripts/client.env not found, creating from example..."
    if [ -f "./scripts/client.env.example" ]; then
        cp "./scripts/client.env.example" "$ENV_FILE"
        echo "Please edit $ENV_FILE with your configuration"
    else
        echo "Error: No environment file template found"
        exit 1
    fi
fi

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload

echo ""
echo "Installation complete!"
echo ""
echo "Next steps:"
echo "1. Edit environment file: nano $ENV_FILE"
echo "2. Ensure CA certificate is at the path specified in CLUSTER_CA_FILE"
echo "3. Start the service: systemctl start $SERVICE_NAME"
echo "4. Enable on boot: systemctl enable $SERVICE_NAME"
echo "5. Check status: systemctl status $SERVICE_NAME"
echo "6. View logs: journalctl -u $SERVICE_NAME -f"

