#!/bin/bash
set -e

# Installation script for UDP Logger Client on Linux server

INSTALL_DIR="/opt/udp-logger"
BINARY_NAME="udp-logger"
SERVICE_NAME="udp-logger-client"
CONFIG_DIR="/etc/udp-logger"
SPOOL_DIR="/var/lib/udp-logger/spool"
CERT_DIR="/etc/udp-logger/certs"
USER="udp-logger"

echo "Installing UDP Logger Client..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Create user if not exists
if ! id "$USER" &>/dev/null; then
    echo "Creating user $USER..."
    useradd -r -s /bin/false -d "$SPOOL_DIR" "$USER"
fi

# Create directories
echo "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p "$SPOOL_DIR"
mkdir -p "$CERT_DIR"

# Set permissions
chown -R "$USER:$USER" "$SPOOL_DIR"
chmod 755 "$SPOOL_DIR"

# Copy binary (assuming it's in current directory)
if [ -f "./$BINARY_NAME" ]; then
    echo "Copying binary..."
    cp "./$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    chmod +x "$INSTALL_DIR/$BINARY_NAME"
    chown root:root "$INSTALL_DIR/$BINARY_NAME"
else
    echo "Warning: Binary $BINARY_NAME not found in current directory"
    echo "Please copy the binary to $INSTALL_DIR/$BINARY_NAME manually"
fi

# Copy environment file template
if [ -f "./scripts/client.env.example" ]; then
    if [ ! -f "$CONFIG_DIR/client.env" ]; then
        echo "Creating environment file from template..."
        cp "./scripts/client.env.example" "$CONFIG_DIR/client.env"
        echo "Please edit $CONFIG_DIR/client.env with your configuration"
    else
        echo "Environment file $CONFIG_DIR/client.env already exists, skipping..."
    fi
else
    echo "Warning: client.env.example not found"
fi

# Install systemd service
if [ -f "./scripts/udp-logger-client.service" ]; then
    echo "Installing systemd service..."
    cp "./scripts/udp-logger-client.service" "/etc/systemd/system/$SERVICE_NAME.service"
    systemctl daemon-reload
    echo "Service installed. To start: systemctl start $SERVICE_NAME"
    echo "To enable on boot: systemctl enable $SERVICE_NAME"
else
    echo "Warning: systemd service file not found"
fi

echo ""
echo "Installation complete!"
echo ""
echo "Next steps:"
echo "1. Edit $CONFIG_DIR/client.env with your Master server address and TLS certificate path"
echo "2. Copy your CA certificate to $CERT_DIR/ca.pem"
echo "3. Start the service: systemctl start $SERVICE_NAME"
echo "4. Check status: systemctl status $SERVICE_NAME"
echo "5. View logs: journalctl -u $SERVICE_NAME -f"

