package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"fmt"

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
	log.Printf("running in client mode: udp=%s master=%s:%d",
		cfg.UDPAddr, cfg.Cluster.MasterAddr, cfg.Cluster.MasterPort)

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

	errCh := make(chan error, 3)
	go func() { errCh <- udpSrv.Run(ctx) }()
	go func() { errCh <- runSpooler(ctx, logCh, sp) }()
	go func() { errCh <- clientSrv.Run(ctx) }()
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

// Master mode: TCP Server (from clients) + UDP (fallback) -> RabbitMQ
func runMasterMode(ctx context.Context, cfg config.Config, m *metrics.Metrics) {
	log.Printf("running in master mode: tcp=%s:%d udp=%s rabbit=%s:%d tls=%v",
		cfg.Cluster.MasterAddr, cfg.Cluster.MasterPort, cfg.UDPAddr, cfg.Rabbit.Host, cfg.Rabbit.Port, cfg.Rabbit.TLS.Enabled)

	logCh := make(chan string, cfg.BufferSize)

	sp, err := spool.Open(cfg.SpoolDir, cfg.SpoolMaxBytes, cfg.SpoolSegmentBytes, cfg.SpoolFsync)
	if err != nil {
		log.Fatalf("spool open error: %v", err)
	}
	defer func() { _ = sp.Close() }()

	// TCP server to receive from clients
	tlsCfg, err := cfg.Cluster.TLSConfig()
	if err != nil {
		log.Fatalf("cluster TLS config error: %v", err)
	}

	masterAddr := cfg.Cluster.MasterAddr
	if masterAddr == "" {
		masterAddr = "0.0.0.0"
	}
	masterSrv := master.Server{
		Addr:      fmt.Sprintf("%s:%d", masterAddr, cfg.Cluster.MasterPort),
		TLSConfig: tlsCfg,
		Out:       logCh,
		Metrics:   m,
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
