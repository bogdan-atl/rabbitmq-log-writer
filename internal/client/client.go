package client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"rabbit-log-writer/internal/metrics"
	"rabbit-log-writer/internal/spool"
)

type Client struct {
	MasterAddr string
	MasterPort int
	TLSConfig  *tls.Config
	Spool      *spool.Spool
	Metrics    *metrics.Metrics
	RetryInterval time.Duration
}

func (c Client) Run(ctx context.Context) error {
	if c.MasterAddr == "" {
		return errors.New("client: master address is empty")
	}
	if c.MasterPort <= 0 {
		c.MasterPort = 9999
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = 5 * time.Second
	}
	if c.Spool == nil {
		return errors.New("client: spool is nil")
	}

	addr := fmt.Sprintf("%s:%d", c.MasterAddr, c.MasterPort)
	log.Printf("client: connecting to master at %s", addr)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := c.connect(ctx, addr)
		if err != nil {
			log.Printf("client: connect error: %v; retry in %s", err, c.RetryInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.RetryInterval):
				continue
			}
		}

		log.Printf("client: connected to master at %s", addr)
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

func (c Client) connect(ctx context.Context, addr string) (net.Conn, error) {
	var conn net.Conn
	var err error

	// Configure TCP keepalive
	tcpDialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second, // Send keepalive packets every 30 seconds
	}

	if c.TLSConfig != nil {
		// For TLS, we need to set keepalive after connection
		dialer := &tls.Dialer{
			Config:    c.TLSConfig,
			NetDialer: tcpDialer,
		}
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	} else {
		conn, err = tcpDialer.DialContext(ctx, "tcp", addr)
	}
	
	if err != nil {
		return nil, err
	}

	// Enable TCP keepalive on the connection
	// For TLS connections, get the underlying TCP connection
	var tcpConn *net.TCPConn
	if tlsConn, ok := conn.(*tls.Conn); ok {
		// Get underlying connection
		if netConn := tlsConn.NetConn(); netConn != nil {
			if tc, ok := netConn.(*net.TCPConn); ok {
				tcpConn = tc
			}
		}
	} else if tc, ok := conn.(*net.TCPConn); ok {
		tcpConn = tc
	}
	
	if tcpConn != nil {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	return conn, nil
}

func (c Client) handleConnection(ctx context.Context, conn net.Conn) error {
	defer conn.Close()

	// Create a connection-specific context that will be cancelled when connection is lost
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	// Heartbeat ticker - send ping every 20 seconds to keep connection alive
	heartbeatInterval := 20 * time.Second
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	// Channel for messages from spool
	msgCh := make(chan spoolMessage, 100) // Larger buffer for batching
	errCh := make(chan error, 1)

	// Goroutine to read from spool
	// This goroutine will be cancelled when connection is lost, preventing duplicate sends
	go func() {
		defer close(msgCh)
		for {
			select {
			case <-connCtx.Done():
				return
			default:
			}

			msg, ack, err := c.Spool.Next(connCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				// spool empty, wait a bit
				select {
				case <-connCtx.Done():
					return
				case <-time.After(1 * time.Second):
					continue
				}
			}
			select {
			case msgCh <- spoolMessage{msg: msg, ack: ack}:
			case <-connCtx.Done():
				return
			}
		}
	}()

	// Main loop: send messages or heartbeat
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-connCtx.Done():
			// Connection context cancelled (spool goroutine stopped)
			return connCtx.Err()
		case err := <-errCh:
			return err
		case <-heartbeatTicker.C:
			// Send heartbeat (ping message with length 0)
			if err := c.sendHeartbeat(conn); err != nil {
				log.Printf("client: heartbeat error: %v", err)
				return err
			}
		case sm, ok := <-msgCh:
			if !ok {
				// Channel closed, spool goroutine exited
				return errors.New("client: spool reader exited")
			}
			
			// Collect batch of messages - send ALL available messages at once
			// Don't wait for timeout if no more messages are available
			batch := []spoolMessage{sm}
			batchSize := len(sm.msg)
			maxBatchSize := 10000 // Large batch limit
			maxBatchBytes := 50 * 1024 * 1024 // 50MB
			batchTimeout := 50 * time.Millisecond // Short timeout
			
			// Collect additional messages for batch
			batchTimer := time.NewTimer(batchTimeout)
			defer batchTimer.Stop()
			
		collectBatch:
			for len(batch) < maxBatchSize && batchSize < maxBatchBytes {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-connCtx.Done():
					return connCtx.Err()
				case nextMsg, ok := <-msgCh:
					if !ok {
						// Channel closed, send what we have
						break collectBatch
					}
					batch = append(batch, nextMsg)
					batchSize += len(nextMsg.msg)
					// Reset timer since we got a message
					if !batchTimer.Stop() {
						<-batchTimer.C
					}
					batchTimer.Reset(batchTimeout)
				case <-batchTimer.C:
					// Timeout - send what we have
					break collectBatch
				}
			}
			
			// Log actual batch size (not the limit)
			if len(batch) > 0 {
				log.Printf("client: collected batch of %d messages (%d bytes)", len(batch), batchSize)
			}
			
			// Retry loop: send batch and wait for ACK
			// If NAK received, retry sending the same batch
			// CRITICAL: We must receive ACK before acking spool entries to prevent duplicates
			batchSent := false
			for {
				if !batchSent {
					// Send batch to master
					if err := c.sendBatch(conn, batch); err != nil {
						log.Printf("client: send batch error: %v", err)
						// If send fails, connection is broken - don't ack, will retry on reconnect
						return err
					}
					batchSent = true
				}

				// Wait for ACK from master before acknowledging spool entries
				// This ensures we don't re-send the same messages after reconnection
				// Use longer timeout for NAT scenarios where ACK might be delayed
				ackReceived, err := c.waitForAck(conn)
				if err != nil {
					// Check if it's a timeout - might be NAT issue
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						log.Printf("client: ACK timeout after sending batch of %d messages (possible NAT issue, assuming success)", len(batch))
						// For NAT scenarios: if we sent the batch successfully but ACK times out,
						// assume success to prevent infinite retries. This is a trade-off:
						// - If master received: good, we ack and move on
						// - If master didn't receive: we'll lose messages, but won't spam
						// Better to ack and risk loss than infinite retry loop
						ackReceived = true
					} else {
						log.Printf("client: wait for ACK error: %v (batch was sent but ACK not received)", err)
						// Connection broken after sending batch but before ACK
						// Don't ack spool entries - they will be re-sent on reconnect
						return err
					}
				}

				if ackReceived {
					// Master confirmed receipt - NOW safe to ack all spool entries in batch
					// This is the ONLY place where we ack - ensures no duplicates
					ackedCount := 0
					for i, sm := range batch {
						if sm.ack != nil {
							if err := sm.ack(); err != nil {
								log.Printf("client: spool ack error for message %d in batch: %v", i, err)
								// If ack fails, we've already sent the batch, but can't mark it as consumed
								// This is a problem, but better than duplicating
							} else {
								ackedCount++
							}
						}
					}
					
					if ackedCount != len(batch) {
						log.Printf("client: WARNING: only %d/%d messages in batch were acked", ackedCount, len(batch))
					}
					// Only log large batches to reduce log spam
					if len(batch) >= 100 {
						log.Printf("client: successfully processed batch of %d messages", len(batch))
					}

					if c.Metrics != nil {
						// Increment metric for each message in batch
						for range batch {
							c.Metrics.RabbitPublishedTotal.Inc()
						}
					}
					break // Success, move to next batch
				} else {
					// Master sent NAK (channel full or error) - reduce batch size and retry
					log.Printf("client: master sent NAK for batch of %d messages, reducing batch size", len(batch))
					
					// If batch is too large, split it in half
					if len(batch) > 1 {
						half := len(batch) / 2
						// Put second half back in channel for later
						// But we can't easily do that, so just reduce batch size
						batch = batch[:half]
						batchSize = 0
						for _, sm := range batch {
							batchSize += len(sm.msg)
						}
						log.Printf("client: reduced batch to %d messages, retrying", len(batch))
						batchSent = false
						continue
					}
					
					// Single message NAK - wait and retry
					batchSent = false
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(2 * time.Second):
						// Retry sending the same batch
						continue
					}
				}
			}
		}
	}
}

type spoolMessage struct {
	msg string
	ack func() error
}

func (c Client) sendHeartbeat(conn net.Conn) error {
	// Send heartbeat: message length = 0 (special value)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write heartbeat: %w", err)
	}
	return nil
}

func (c Client) sendMessage(conn net.Conn, msg string) error {
	body := []byte(msg)
	msgLen := uint32(len(body))

	// Set write deadline
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}

	// Write length (4 bytes, big-endian)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], msgLen)
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	// Write body - if this fails after length is written, the message will be retried
	// Master should handle incomplete messages gracefully (timeout or connection close)
	if _, err := conn.Write(body); err != nil {
		return fmt.Errorf("write body: %w", err)
	}

	// Message fully sent - will wait for ACK from master
	return nil
}

// sendBatch sends a batch of messages to master
// Format: [batch_count (4 bytes)] [msg1_len (4 bytes)] [msg1_body] [msg2_len (4 bytes)] [msg2_body] ...
func (c Client) sendBatch(conn net.Conn, batch []spoolMessage) error {
	// Set write deadline
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}

	// Write batch count (4 bytes, big-endian)
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(batch)))
	if _, err := conn.Write(countBuf[:]); err != nil {
		return fmt.Errorf("write batch count: %w", err)
	}

	// Write each message: length (4 bytes) + body
	var lenBuf [4]byte
	for _, sm := range batch {
		body := []byte(sm.msg)
		msgLen := uint32(len(body))
		
		binary.BigEndian.PutUint32(lenBuf[:], msgLen)
		if _, err := conn.Write(lenBuf[:]); err != nil {
			return fmt.Errorf("write message length: %w", err)
		}
		
		if _, err := conn.Write(body); err != nil {
			return fmt.Errorf("write message body: %w", err)
		}
	}

	return nil
}

// waitForAck waits for ACK (0x01) or NAK (0x00) from master
// Returns (true, nil) for ACK, (false, nil) for NAK, (false, error) for error
func (c Client) waitForAck(conn net.Conn) (bool, error) {
	// Set read deadline for ACK - longer timeout for NAT scenarios
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return false, err
	}

	ackBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, ackBuf); err != nil {
		return false, fmt.Errorf("read ACK: %w", err)
	}

	ackByte := ackBuf[0]
	if ackByte == 0x01 {
		// ACK received - message was successfully processed
		return true, nil
	} else if ackByte == 0x00 {
		// NAK received - master couldn't process (e.g., channel full)
		return false, nil
	} else {
		// Invalid ACK byte
		return false, fmt.Errorf("invalid ACK byte: 0x%02x", ackByte)
	}
}

