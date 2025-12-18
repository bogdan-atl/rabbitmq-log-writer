package udp

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	"rabbit-log-writer/internal/metrics"
)

type Server struct {
	Addr         string
	ReadBufBytes int
	Out          chan<- string

	DropEvery int // log a drop warning every N dropped messages; 0 disables
	Metrics   *metrics.Metrics
}

func (s Server) Run(ctx context.Context) error {
	if s.Addr == "" {
		s.Addr = ":516"
	}
	if s.ReadBufBytes <= 0 {
		s.ReadBufBytes = 1024
	}
	if s.DropEvery <= 0 {
		s.DropEvery = 1000
	}
	if s.Out == nil {
		return errors.New("udp server: Out channel is nil")
	}

	pc, err := net.ListenPacket("udp", s.Addr)
	if err != nil {
		return err
	}
	defer pc.Close()

	log.Printf("udp listening on %s", s.Addr)

	buf := make([]byte, s.ReadBufBytes)
	var dropped uint64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read deadline so we can react to ctx cancellation.
		_ = pc.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		msg := string(buf[:n])
		if s.Metrics != nil {
			s.Metrics.UDPReceivedTotal.Inc()
		}

		select {
		case s.Out <- msg:
		default:
			dropped++
			if s.Metrics != nil {
				s.Metrics.UDPDroppedTotal.Inc()
			}
			if s.DropEvery > 0 && dropped%uint64(s.DropEvery) == 0 {
				log.Printf("udp buffer full: dropped=%d (last drop sample=%q)", dropped, trimSample(msg, 120))
			}
		}
	}
}

func trimSample(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max] + "..."
}


