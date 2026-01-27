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
	"rabbit-log-writer/internal/spool"
)

// WebSocketClient uses WebSocket instead of raw TCP
type WebSocketClient struct {
	MasterURL     string      // e.g., "ws://master:9999" or "wss://master:9999"
	TLSConfig     *tls.Config // TLS config for wss:// connections
	Spool         *spool.Spool
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
	if c.Spool == nil {
		return errors.New("client: spool is nil")
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

	// Main loop: read all available messages from spool and send them at once
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-connCtx.Done():
			return connCtx.Err()
		default:
		}

		// Collect ALL available messages from spool at once
		var batch []spoolMessage
		batchSize := 0
		maxBatchBytes := 50 * 1024 * 1024 // 50MB max to prevent memory issues
		
		// Read messages until spool is empty or we hit size limit
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-connCtx.Done():
				return connCtx.Err()
			default:
			}

			// Try to read next message with timeout
			readCtx, cancelRead := context.WithTimeout(connCtx, 100*time.Millisecond)
			msg, ack, err := c.Spool.Next(readCtx)
			cancelRead()
			
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// No more messages available or timeout
					break
				}
				// Other error - wait a bit and retry
				select {
				case <-connCtx.Done():
					return connCtx.Err()
				case <-time.After(1 * time.Second):
					continue
				}
			}

			msgSize := len(msg)
			// Check if adding this message would exceed size limit
			if batchSize+msgSize > maxBatchBytes {
				// Send current batch first, then continue with this message
				if len(batch) > 0 {
					break
				}
				// Single message too large - skip it (shouldn't happen)
				log.Printf("client: WARNING: message too large (%d bytes), skipping", msgSize)
				if ack != nil {
					ack() // Still ack it to remove from spool
				}
				continue
			}

			batch = append(batch, spoolMessage{msg: msg, ack: ack})
			batchSize += msgSize
		}

		// If we have messages, send them all at once
		if len(batch) > 0 {
			log.Printf("client: collected %d messages (%d bytes), sending all at once", len(batch), batchSize)
			
			// Prepare batch data
			batchData := make([]string, len(batch))
			for i, sm := range batch {
				batchData[i] = sm.msg
			}
			
			message := map[string]interface{}{
				"type":     "batch",
				"count":    len(batch),
				"messages": batchData,
			}
			
			// Send batch
			conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
			if err := conn.WriteJSON(message); err != nil {
				log.Printf("client: send batch error: %v (batch size: %d)", err, len(batch))
				// On error, don't ack - messages will remain in spool for retry
				// Connection will be closed and reconnected
				return err
			}
			
			// Successfully sent - ack all messages in batch immediately
			// Messages are now in master's buffer, so we can remove them from spool
			ackedCount := 0
			for i, sm := range batch {
				if sm.ack != nil {
					if err := sm.ack(); err != nil {
						log.Printf("client: spool ack error for message %d: %v", i, err)
					} else {
						ackedCount++
					}
				}
			}
			
			if ackedCount != len(batch) {
				log.Printf("client: WARNING: acked %d/%d messages in batch", ackedCount, len(batch))
			} else {
				log.Printf("client: successfully sent and acked %d messages", len(batch))
			}
			
			if c.Metrics != nil {
				for range batch {
					c.Metrics.RabbitPublishedTotal.Inc()
				}
			}
		} else {
			// No messages available - wait a bit before checking again
			select {
			case <-connCtx.Done():
				return connCtx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}
	}
}

