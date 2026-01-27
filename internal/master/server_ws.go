package master

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"rabbit-log-writer/internal/metrics"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins
	},
	ReadBufferSize:  1024 * 1024, // 1MB
	WriteBufferSize: 1024 * 1024, // 1MB
}

// WebSocketServer uses WebSocket instead of raw TCP
type WebSocketServer struct {
	Addr      string // e.g., ":9999"
	TLSConfig *tls.Config
	Out       chan<- string
	Metrics   *metrics.Metrics
}

func (s WebSocketServer) Run(ctx context.Context) error {
	if s.Addr == "" {
		s.Addr = ":9999"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleWebSocket)

	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}

	// Configure TLS if provided
	if s.TLSConfig != nil {
		server.TLSConfig = s.TLSConfig
		log.Printf("master websocket server listening on %s (TLS enabled)", s.Addr)
	} else {
		log.Printf("master websocket server listening on %s (no TLS)", s.Addr)
	}

	// Shutdown server on context cancellation
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	var err error
	if s.TLSConfig != nil {
		// Check if we have server certificates
		if len(s.TLSConfig.Certificates) == 0 {
			log.Printf("master: WARNING: TLS enabled but no server certificates found, falling back to non-TLS WebSocket")
			err = server.ListenAndServe()
		} else {
			// Use TLS listener
			listener, listenErr := tls.Listen("tcp", s.Addr, s.TLSConfig)
			if listenErr != nil {
				log.Printf("master: TLS listen error: %v, falling back to non-TLS", listenErr)
				err = server.ListenAndServe()
			} else {
				err = server.Serve(listener)
			}
		}
	} else {
		err = server.ListenAndServe()
	}
	
	if err != nil && err != http.ErrServerClosed {
		return err
	}

	return ctx.Err()
}

func (s WebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("master: websocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	remoteAddr := r.RemoteAddr
	log.Printf("master: client connected from %s", remoteAddr)

	// Increment connected clients counter
	if s.Metrics != nil {
		s.Metrics.MasterClientsConnected.Inc()
		defer s.Metrics.MasterClientsConnected.Dec()
	}

	// Set read/write deadlines
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// Enable pong handler
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Send ping periodically
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	go func() {
		for {
			select {
			case <-pingTicker.C:
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	// Handle messages
	for {
		var msg map[string]interface{}
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("master: websocket read error from %s: %v", remoteAddr, err)
			}
			return
		}

		msgType, _ := msg["type"].(string)
		if msgType != "batch" {
			log.Printf("master: unexpected message type: %s", msgType)
			continue
		}

		messages, ok := msg["messages"].([]interface{})
		if !ok {
			log.Printf("master: invalid messages format from %s", remoteAddr)
			continue
		}

		// Process all messages in batch - send to output channel
		// No ACK/NAK needed - client sends without waiting for confirmation
		for _, msgInterface := range messages {
			msgStr, ok := msgInterface.(string)
			if !ok {
				continue
			}

			select {
			case s.Out <- msgStr:
				if s.Metrics != nil {
					s.Metrics.UDPReceivedTotal.Inc()
				}
			case <-r.Context().Done():
				return
			default:
				// Channel full - block until there's space
				// This ensures no messages are lost
				select {
				case s.Out <- msgStr:
					if s.Metrics != nil {
						s.Metrics.UDPReceivedTotal.Inc()
					}
				case <-r.Context().Done():
					return
				}
			}
		}
	}
}

