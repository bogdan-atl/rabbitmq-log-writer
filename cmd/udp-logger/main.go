package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rabbit-log-writer/internal/config"
	"rabbit-log-writer/internal/httpserver"
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

	log.Printf("starting udp-logger udp=%s queue=%s rabbit=%s:%d tls=%v",
		cfg.UDPAddr, cfg.QueueName, cfg.Rabbit.Host, cfg.Rabbit.Port, cfg.Rabbit.TLS.Enabled)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// bounded buffer to decouple UDP reads from RabbitMQ availability
	logCh := make(chan string, cfg.BufferSize)

	m := metrics.New()

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

	httpSrv := httpserver.Server{
		Addr:    cfg.HTTPAddr,
		Metrics: m,
	}

	errCh := make(chan error, 2)
	go func() { errCh <- udpSrv.Run(ctx) }()
	go func() { errCh <- runSpooler(ctx, logCh, sp) }()
	go func() { errCh <- pub.Run(ctx, sp) }()
	go func() { _ = httpSrv.Run(ctx) }()
	go func() { _ = runSpoolReporter(ctx, sp, m, cfg.SpoolLogInterval) }()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stopped with error: %v", err)
		}
	}

	// allow goroutines to exit gracefully
	stop()
	time.Sleep(200 * time.Millisecond)
	log.Printf("exited")
	os.Exit(0)
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
				// 如果磁盘满/单条过大等情况，按日志服务常规做法：记录错误继续跑
				log.Printf("spool enqueue error: %v", err)
			}
		}
	}
}

func runSpoolReporter(ctx context.Context, s *spool.Spool, m *metrics.Metrics, interval time.Duration) error {
	// interval=0 disables periodic logs, metrics are still updated on tick (every 5s)
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
				// convert delta dropped into counter increments
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


