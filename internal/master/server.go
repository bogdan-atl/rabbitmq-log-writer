package master

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"rabbit-log-writer/internal/metrics"
)

type Server struct {
	Addr      string
	TLSConfig *tls.Config
	Out       chan<- string
	Metrics   *metrics.Metrics
}

func (s Server) Run(ctx context.Context) error {
	if s.Addr == "" {
		s.Addr = ":9999"
	}

	var ln net.Listener
	var err error
	if s.TLSConfig != nil {
		ln, err = tls.Listen("tcp", s.Addr, s.TLSConfig)
		log.Printf("master tls server listening on %s", s.Addr)
	} else {
		ln, err = net.Listen("tcp", s.Addr)
		log.Printf("master tcp server listening on %s (no TLS)", s.Addr)
	}
	if err != nil {
		return err
	}
	defer ln.Close()

	var wg sync.WaitGroup
	connCh := make(chan net.Conn, 10)

	// Accept connections
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					log.Printf("master accept error: %v", err)
					continue
				}
			}
			select {
			case connCh <- conn:
			case <-ctx.Done():
				conn.Close()
				return
			}
		}
	}()

	// Handle connections
	for {
		select {
		case <-ctx.Done():
			ln.Close()
			wg.Wait()
			return ctx.Err()
		case conn := <-connCh:
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				s.handleConnection(ctx, c)
			}(conn)
		}
	}
}

func (s Server) handleConnection(ctx context.Context, conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("master: client connected from %s", remoteAddr)
	
	// Increment connected clients counter
	if s.Metrics != nil {
		s.Metrics.MasterClientsConnected.Inc()
	}
	
	defer func() {
		log.Printf("master: client disconnected from %s", remoteAddr)
		// Decrement connected clients counter
		if s.Metrics != nil {
			s.Metrics.MasterClientsConnected.Dec()
		}
	}()

	// Enable TCP keepalive
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// Set read deadline (increased to 60 seconds to allow for heartbeat)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read batch count or message length (4 bytes, big-endian)
		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			// Check if it's a timeout - might be normal if no messages
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Reset deadline and continue (allow keepalive to work)
				_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
				continue
			}
			log.Printf("master: read length error from %s: %v", remoteAddr, err)
			return
		}
		firstValue := int64(binary.BigEndian.Uint32(lenBuf[:]))
		
		// Handle heartbeat (length = 0)
		if firstValue == 0 {
			// Heartbeat received, just reset deadline and continue
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			continue
		}
		
		// Check if this is a batch (count > 1 and reasonable) or single message
		// Increased limit to support very large batches
		isBatch := firstValue > 1 && firstValue <= 10000
		
		var messages []string
		if isBatch {
			// This is a batch: read all messages
			batchCount := int(firstValue)
			messages = make([]string, 0, batchCount)
			
			for i := 0; i < batchCount; i++ {
				// Read message length
				if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
					log.Printf("master: read message length error from %s: %v", remoteAddr, err)
					return
				}
				msgLen := int64(binary.BigEndian.Uint32(lenBuf[:]))
				
				if msgLen < 0 || msgLen > 1024*1024 { // max 1MB per message
					log.Printf("master: invalid message length %d in batch from %s", msgLen, remoteAddr)
					return
				}
				
				// Read message body
				body := make([]byte, msgLen)
				if _, err := io.ReadFull(conn, body); err != nil {
					log.Printf("master: read message body error from %s: %v", remoteAddr, err)
					return
				}
				
				messages = append(messages, string(body))
			}
		} else {
			// Single message (legacy format or large message)
			msgLen := firstValue
			
			if msgLen < 0 || msgLen > 1024*1024 { // max 1MB per message
				log.Printf("master: invalid message length %d from %s", msgLen, remoteAddr)
				return
			}

			// Read message body
			body := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, body); err != nil {
				log.Printf("master: read body error from %s: %v", remoteAddr, err)
				return
			}

			messages = []string{string(body)}
		}

		// Process all messages in batch
		allSent := true
		for _, msg := range messages {
			select {
			case s.Out <- msg:
				if s.Metrics != nil {
					s.Metrics.UDPReceivedTotal.Inc() // reuse metric for received messages
				}
			case <-ctx.Done():
				return
			default:
				// Channel full - mark as failed
				log.Printf("master: output channel full, dropping message from %s", remoteAddr)
				if s.Metrics != nil {
					s.Metrics.UDPDroppedTotal.Inc()
				}
				allSent = false
				break
			}
		}

		// Send ACK or NAK based on whether all messages were sent
		var ackByte byte
		if allSent {
			ackByte = 0x01 // ACK
		} else {
			ackByte = 0x00 // NAK
		}
		
		ackBuf := []byte{ackByte}
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err == nil {
			if _, err := conn.Write(ackBuf); err != nil {
				log.Printf("master: write ACK/NAK error to %s: %v", remoteAddr, err)
				return
			}
		}

		// Reset read deadline
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	}
}

