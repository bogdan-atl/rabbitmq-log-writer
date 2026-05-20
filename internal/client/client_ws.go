package client

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"rabbit-log-writer/internal/metrics"
	"rabbit-log-writer/internal/queue"
)

// WebSocketClient uses WebSocket instead of raw TCP
type WebSocketClient struct {
	MasterURL     string      // e.g., "ws://master:9999" or "wss://master:9999"
	TLSConfig     *tls.Config // TLS config for wss:// connections
	Queue         queue.Queue
	Metrics       *metrics.Metrics
	RetryInterval time.Duration
}

func (c WebSocketClient) Run(ctx context.Context) error {
	if c.MasterURL == "" {
		return errors.New("client: master URL is empty")
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = 5 * time.Second
	}
	if c.Queue == nil {
		return errors.New("client: queue is nil")
	}

	log.Printf("client: connecting to master at %s", c.MasterURL)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, _, err := c.connect(ctx, c.MasterURL)
		if err != nil {
			log.Printf("client: connect error: %v; retry in %s", err, c.RetryInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.RetryInterval):
				continue
			}
		}

		log.Printf("client: connected to master at %s", c.MasterURL)
		if err := c.handleConnection(ctx, conn); err != nil {
			log.Printf("client: connection error: %v; reconnecting in %s", err, c.RetryInterval)
			conn.Close()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.RetryInterval):
				continue
			}
		}
	}
}

func (c WebSocketClient) connect(ctx context.Context, url string) (*websocket.Conn, *http.Response, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		TLSClientConfig:  c.TLSConfig, // Use TLS config if provided (for wss://)
	}
	
	// Add headers for WebSocket upgrade
	headers := make(http.Header)
	headers.Set("User-Agent", "udp-logger-client/1.0")
	
	conn, resp, err := dialer.DialContext(ctx, url, headers)
	if err != nil {
		if resp != nil {
			log.Printf("client: websocket handshake failed: %v (status: %d, headers: %v)", err, resp.StatusCode, resp.Header)
			// Log response body for debugging
			if resp.Body != nil {
				body := make([]byte, 512)
				if n, _ := resp.Body.Read(body); n > 0 {
					log.Printf("client: server response body: %s", string(body[:n]))
				}
				resp.Body.Close()
			}
		} else {
			log.Printf("client: websocket connection failed: %v (no response)", err)
		}
		return nil, resp, err
	}

	// Set read/write deadlines
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	
	// Enable pong handler for keepalive
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	return conn, resp, nil
}

func (c WebSocketClient) handleConnection(ctx context.Context, conn *websocket.Conn) error {
	defer conn.Close()

	// Create a connection-specific context
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	// Main loop: read one message, send it, then ack.
	// This preserves "unsent messages stay in queue" semantics.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-connCtx.Done():
			return connCtx.Err()
		default:
		}

		msg, ack, err := c.Queue.Next(connCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return connCtx.Err()
			}
			select {
			case <-connCtx.Done():
				return connCtx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}

		message := map[string]interface{}{
			"type":     "batch",
			"count":    1,
			"messages": []string{msg},
		}

		conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
		if err := conn.WriteJSON(message); err != nil {
			// On write failure message is not acked and will be retried.
			log.Printf("client: send batch error: %v (batch size: 1)", err)
			return err
		}

		if ack != nil {
			if err := ack(); err != nil {
				log.Printf("client: queue ack error: %v", err)
			}
		}
		if c.Metrics != nil {
			c.Metrics.RabbitPublishedTotal.Inc()
		}
	}
}

