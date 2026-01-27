package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"rabbit-log-writer/internal/client"
	"rabbit-log-writer/internal/config"
	"rabbit-log-writer/internal/httpserver"
	"rabbit-log-writer/internal/master"
	"rabbit-log-writer/internal/metrics"
	"rabbit-log-writer/internal/rabbit"
	"rabbit-log-writer/internal/spool"
	"rabbit-log-writer/internal/udp"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	mode := strings.ToLower(cfg.Cluster.Mode)
	if mode == "" {
		mode = "standalone"
	}
	log.Printf("starting udp-logger mode=%s udp=%s queue=%s", mode, cfg.UDPAddr, cfg.QueueName)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m := metrics.New(mode)

	httpSrv := httpserver.Server{
		Addr:    cfg.HTTPAddr,
		Metrics: m,
	}
	go func() { _ = httpSrv.Run(ctx) }()

	switch mode {
	case "client":
		runClientMode(ctx, cfg, m)
	case "master":
		runMasterMode(ctx, cfg, m)
	default:
		runStandaloneMode(ctx, cfg, m)
	}

	stop()
	time.Sleep(200 * time.Millisecond)
	log.Printf("exited")
	os.Exit(0)
}

// Standalone mode: UDP -> Spool -> RabbitMQ (original behavior)
func runStandaloneMode(ctx context.Context, cfg config.Config, m *metrics.Metrics) {
	log.Printf("running in standalone mode: udp=%s rabbit=%s:%d tls=%v",
		cfg.UDPAddr, cfg.Rabbit.Host, cfg.Rabbit.Port, cfg.Rabbit.TLS.Enabled)

	logCh := make(chan string, cfg.BufferSize)

	sp, err := spool.Open(cfg.SpoolDir, cfg.SpoolMaxBytes, cfg.SpoolSegmentBytes, cfg.SpoolFsync)
	if err != nil {
		log.Fatalf("spool open error: %v", err)
	}
	defer func() { _ = sp.Close() }()

	udpSrv := udp.Server{
		Addr:         cfg.UDPAddr,
		ReadBufBytes: cfg.UDPReadBufBytes,
		Out:          logCh,
		Metrics:      m,
	}

	pub := rabbit.Publisher{
		Config:            cfg.Rabbit,
		QueueName:         cfg.QueueName,
		PublishInterval:   cfg.PublishRetryInterval,
		DropLogEvery:      1000,
		TimestampLocation: time.Local,
		Metrics:           m,
	}

	errCh := make(chan error, 3)
	go func() { errCh <- udpSrv.Run(ctx) }()
	go func() { errCh <- runSpooler(ctx, logCh, sp) }()
	go func() { errCh <- pub.Run(ctx, sp) }()
	go func() { _ = runSpoolReporter(ctx, sp, m, cfg.SpoolLogInterval) }()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stopped with error: %v", err)
		}
	}
}

// Client mode: UDP -> Spool -> TCP Client -> Master
func runClientMode(ctx context.Context, cfg config.Config, m *metrics.Metrics) {
	log.Printf("running in client mode: udp=%s master=%s:%d protocol=%s tls=%v",
		cfg.UDPAddr, cfg.Cluster.MasterAddr, cfg.Cluster.MasterPort, cfg.Cluster.Protocol, cfg.Cluster.TLS.Enabled)

	logCh := make(chan string, cfg.BufferSize)

	sp, err := spool.Open(cfg.SpoolDir, cfg.SpoolMaxBytes, cfg.SpoolSegmentBytes, cfg.SpoolFsync)
	if err != nil {
		log.Fatalf("spool open error: %v", err)
	}
	defer func() { _ = sp.Close() }()

	udpSrv := udp.Server{
		Addr:         cfg.UDPAddr,
		ReadBufBytes: cfg.UDPReadBufBytes,
		Out:          logCh,
		Metrics:      m,
	}

	protocol := cfg.Cluster.Protocol
	if protocol == "" {
		protocol = "ws" // Default to WebSocket
	}

	errCh := make(chan error, 3)
	go func() { errCh <- udpSrv.Run(ctx) }()
	go func() { errCh <- runSpooler(ctx, logCh, sp) }()
	go func() { _ = runSpoolReporter(ctx, sp, m, cfg.SpoolLogInterval) }()

	if protocol == "ws" || protocol == "websocket" {
		// Use WebSocket client
		// Determine if we should use wss:// or ws://
		// If CLUSTER_TLS is enabled, use wss:// (even if we only have CA file)
		masterURL := fmt.Sprintf("ws://%s:%d", cfg.Cluster.MasterAddr, cfg.Cluster.MasterPort)
		var tlsCfg *tls.Config
		
		if cfg.Cluster.TLS.Enabled {
			// TLS is enabled - use wss://
			masterURL = fmt.Sprintf("wss://%s:%d", cfg.Cluster.MasterAddr, cfg.Cluster.MasterPort)
			var err error
			tlsCfg, err = cfg.Cluster.TLSConfig()
			if err != nil {
				log.Printf("cluster TLS config error: %v, but will still try wss://", err)
				// Create minimal TLS config for wss:// connection
				tlsCfg = &tls.Config{
					InsecureSkipVerify: cfg.Cluster.TLS.InsecureSkipVerify,
					ServerName:          cfg.Cluster.TLS.ServerName,
				}
				if cfg.Cluster.TLS.CAFile != "" {
					if caPem, readErr := os.ReadFile(cfg.Cluster.TLS.CAFile); readErr == nil {
						pool := x509.NewCertPool()
						if pool.AppendCertsFromPEM(caPem) {
							tlsCfg.RootCAs = pool
						}
					}
				}
			}
			log.Printf("client: using wss:// for WebSocket connection (TLS enabled)")
		} else {
			log.Printf("client: using ws:// for WebSocket connection (TLS disabled)")
		}
		wsClient := client.WebSocketClient{
			MasterURL:    masterURL,
			TLSConfig:    tlsCfg,
			Spool:        sp,
			Metrics:      m,
			RetryInterval: cfg.PublishRetryInterval,
		}
		go func() { errCh <- wsClient.Run(ctx) }()
	} else {
		// Use TCP client
		tlsCfg, err := cfg.Cluster.TLSConfig()
		if err != nil {
			log.Fatalf("cluster TLS config error: %v", err)
		}
		clientSrv := client.Client{
			MasterAddr:   cfg.Cluster.MasterAddr,
			MasterPort:   cfg.Cluster.MasterPort,
			TLSConfig:    tlsCfg,
			Spool:        sp,
			Metrics:      m,
			RetryInterval: cfg.PublishRetryInterval,
		}
		go func() { errCh <- clientSrv.Run(ctx) }()
	}

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stopped with error: %v", err)
		}
	}
}

// Master mode: WebSocket/TCP Server (from clients) + UDP (fallback) -> RabbitMQ
func runMasterMode(ctx context.Context, cfg config.Config, m *metrics.Metrics) {
	protocol := cfg.Cluster.Protocol
	if protocol == "" {
		protocol = "ws" // Default to WebSocket
	}
	log.Printf("running in master mode: protocol=%s addr=%s:%d udp=%s rabbit=%s:%d tls=%v",
		protocol, cfg.Cluster.MasterAddr, cfg.Cluster.MasterPort, cfg.UDPAddr, cfg.Rabbit.Host, cfg.Rabbit.Port, cfg.Rabbit.TLS.Enabled)

	logCh := make(chan string, cfg.BufferSize)

	sp, err := spool.Open(cfg.SpoolDir, cfg.SpoolMaxBytes, cfg.SpoolSegmentBytes, cfg.SpoolFsync)
	if err != nil {
		log.Fatalf("spool open error: %v", err)
	}
	defer func() { _ = sp.Close() }()

	var masterSrv interface {
		Run(context.Context) error
	}

	if protocol == "ws" || protocol == "websocket" {
		// Use WebSocket server
		masterAddr := cfg.Cluster.MasterAddr
		if masterAddr == "" {
			masterAddr = "0.0.0.0"
		}
		tlsCfg, err := cfg.Cluster.TLSConfig()
		if err != nil {
			log.Fatalf("cluster TLS config error: %v", err)
		}
		wsServer := master.WebSocketServer{
			Addr:      fmt.Sprintf("%s:%d", masterAddr, cfg.Cluster.MasterPort),
			TLSConfig: tlsCfg,
			Out:       logCh,
			Metrics:   m,
		}
		masterSrv = wsServer
	} else {
		// Use TCP server
		tlsCfg, err := cfg.Cluster.TLSConfig()
		if err != nil {
			log.Fatalf("cluster TLS config error: %v", err)
		}
		masterAddr := cfg.Cluster.MasterAddr
		if masterAddr == "" {
			masterAddr = "0.0.0.0"
		}
		tcpServer := master.Server{
			Addr:      fmt.Sprintf("%s:%d", masterAddr, cfg.Cluster.MasterPort),
			TLSConfig: tlsCfg,
			Out:       logCh,
			Metrics:   m,
		}
		masterSrv = tcpServer
	}

	// UDP server as fallback (if clients are down)
	udpSrv := udp.Server{
		Addr:         cfg.UDPAddr,
		ReadBufBytes: cfg.UDPReadBufBytes,
		Out:          logCh,
		Metrics:      m,
	}

	// RabbitMQ publisher
	pub := rabbit.Publisher{
		Config:            cfg.Rabbit,
		QueueName:         cfg.QueueName,
		PublishInterval:   cfg.PublishRetryInterval,
		DropLogEvery:      1000,
		TimestampLocation: time.Local,
		Metrics:           m,
	}

	errCh := make(chan error, 4)
	go func() { errCh <- masterSrv.Run(ctx) }()
	go func() { errCh <- udpSrv.Run(ctx) }()
	go func() { errCh <- runSpooler(ctx, logCh, sp) }()
	go func() { errCh <- pub.Run(ctx, sp) }()
	go func() { _ = runSpoolReporter(ctx, sp, m, cfg.SpoolLogInterval) }()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stopped with error: %v", err)
		}
	}
}

func runSpooler(ctx context.Context, in <-chan string, s *spool.Spool) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-in:
			if !ok {
				return nil
			}
			if err := s.Enqueue(msg); err != nil {
				log.Printf("spool enqueue error: %v", err)
			}
		}
	}
}

func runSpoolReporter(ctx context.Context, s *spool.Spool, m *metrics.Metrics, interval time.Duration) error {
	tick := 5 * time.Second
	t := time.NewTicker(tick)
	defer t.Stop()

	var lastDropped int64
	var lastLog time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			st := s.Stats()
			if m != nil {
				m.SpoolQueued.Set(float64(st.Queued))
				m.SpoolBytes.Set(float64(st.Bytes))
				if st.Dropped > lastDropped {
					m.SpoolDroppedTotal.Add(float64(st.Dropped - lastDropped))
					lastDropped = st.Dropped
				}
			}

			if interval > 0 {
				if lastLog.IsZero() || time.Since(lastLog) >= interval {
					log.Printf("spool buffered messages=%d bytes=%d readSeg=%d writeSeg=%d", st.Queued, st.Bytes, st.ReadSeg, st.WriteSeg)
					lastLog = time.Now()
				}
			}
		}
	}
}
