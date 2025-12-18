package httpserver

import (
	"context"
	"log"
	"net/http"
	"time"

	"rabbit-log-writer/internal/metrics"
)

type Server struct {
	Addr    string
	Metrics *metrics.Metrics
}

func (s Server) Run(ctx context.Context) error {
	addr := s.Addr
	if addr == "" {
		addr = ":9793"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if s.Metrics != nil {
		mux.Handle("/metrics", s.Metrics.Handler())
	} else {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("# metrics disabled\n"))
		})
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("http server listening on %s", addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}



