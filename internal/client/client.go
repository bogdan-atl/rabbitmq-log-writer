package client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
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

	// Heartbeat ticker - send ping every 20 seconds to keep connection alive
	heartbeatInterval := 20 * time.Second
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	// Channel for messages from spool
	msgCh := make(chan spoolMessage, 1)
	errCh := make(chan error, 1)

	// Goroutine to read from spool
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, ack, err := c.Spool.Next(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					errCh <- err
					return
				}
				// spool empty, wait a bit
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
					continue
				}
			}
			select {
			case msgCh <- spoolMessage{msg: msg, ack: ack}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Main loop: send messages or heartbeat
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-heartbeatTicker.C:
			// Send heartbeat (ping message with length 0)
			if err := c.sendHeartbeat(conn); err != nil {
				log.Printf("client: heartbeat error: %v", err)
				return err
			}
		case sm := <-msgCh:
			// Send message to master
			if err := c.sendMessage(conn, sm.msg); err != nil {
				log.Printf("client: send error: %v", err)
				return err
			}

			// Ack spool entry only after successful send
			if sm.ack != nil {
				if err := sm.ack(); err != nil {
					log.Printf("client: spool ack error: %v", err)
				}
			}

		if c.Metrics != nil {
			// In client mode, this represents messages sent to master, not RabbitMQ
			// We reuse the metric but it's labeled with mode=client
			c.Metrics.RabbitPublishedTotal.Inc()
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

	// Message fully sent - will be acked by caller
	return nil
}

