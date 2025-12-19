package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	UDPReceivedTotal prometheus.Counter
	UDPDroppedTotal  prometheus.Counter

	RabbitPublishedTotal    prometheus.Counter
	RabbitConnectErrorsTotal prometheus.Counter
	RabbitPublishErrorsTotal prometheus.Counter
	RabbitConnected          prometheus.Gauge

	SpoolQueued prometheus.Gauge
	SpoolBytes  prometheus.Gauge
	SpoolDroppedTotal prometheus.Counter

	MasterClientsConnected prometheus.Gauge
}

func New(mode string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	// Common labels for all metrics
	labels := prometheus.Labels{"mode": mode}

	m := &Metrics{
		registry: reg,
		UDPReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "udp_logger_udp_received_total",
			Help:        "Total number of UDP datagrams received.",
			ConstLabels: labels,
		}),
		UDPDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "udp_logger_udp_dropped_total",
			Help:        "Total number of UDP datagrams dropped due to full buffer.",
			ConstLabels: labels,
		}),
		RabbitPublishedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "udp_logger_rabbit_published_total",
			Help:        "Total number of messages successfully published to RabbitMQ.",
			ConstLabels: labels,
		}),
		RabbitConnectErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "udp_logger_rabbit_connect_errors_total",
			Help:        "Total number of RabbitMQ connect/setup errors.",
			ConstLabels: labels,
		}),
		RabbitPublishErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "udp_logger_rabbit_publish_errors_total",
			Help:        "Total number of RabbitMQ publish errors.",
			ConstLabels: labels,
		}),
		RabbitConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "udp_logger_rabbit_connected",
			Help:        "1 if connected to RabbitMQ, otherwise 0.",
			ConstLabels: labels,
		}),
		SpoolQueued: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "udp_logger_spool_queued",
			Help:        "Number of messages currently queued in local spool.",
			ConstLabels: labels,
		}),
		SpoolBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "udp_logger_spool_bytes",
			Help:        "Total bytes currently used by local spool segment files.",
			ConstLabels: labels,
		}),
		SpoolDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "udp_logger_spool_dropped_total",
			Help:        "Total number of messages dropped from spool due to max size (drop-oldest).",
			ConstLabels: labels,
		}),
		MasterClientsConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "udp_logger_master_clients_connected",
			Help:        "Number of clients currently connected to master server.",
			ConstLabels: labels,
		}),
	}

	reg.MustRegister(
		m.UDPReceivedTotal,
		m.UDPDroppedTotal,
		m.RabbitPublishedTotal,
		m.RabbitConnectErrorsTotal,
		m.RabbitPublishErrorsTotal,
		m.RabbitConnected,
		m.SpoolQueued,
		m.SpoolBytes,
		m.SpoolDroppedTotal,
		m.MasterClientsConnected,
	)

	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}



