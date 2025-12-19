package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	UDPAddr             string
	UDPReadBufBytes     int
	HTTPAddr            string
	QueueName           string
	BufferSize          int
	PublishRetryInterval time.Duration
	SpoolDir            string
	SpoolMaxBytes       int64
	SpoolSegmentBytes   int64
	SpoolFsync          bool
	SpoolLogInterval    time.Duration
	Rabbit              RabbitConfig
	Cluster             ClusterConfig
}

type ClusterConfig struct {
	Mode      string // "client" or "master" or "" (standalone)
	MasterAddr string
	MasterPort int
	TLS        ClusterTLSConfig
}

type ClusterTLSConfig struct {
	Enabled    bool
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
	InsecureSkipVerify bool
}

type RabbitTLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

type RabbitConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	VHost    string
	TLS      RabbitTLSConfig
}

func LoadFromEnv() (Config, error) {
	var cfg Config

	cfg.UDPAddr = getEnv("UDP_ADDR", ":516")
	cfg.HTTPAddr = getEnv("HTTP_ADDR", ":9793")
	cfg.QueueName = getEnv("QUEUE_NAME", "mikrotik")

	cfg.UDPReadBufBytes = getEnvInt("UDP_READ_BUFFER", 1024)
	cfg.BufferSize = getEnvInt("BUFFER_SIZE", 1000)
	cfg.PublishRetryInterval = getEnvDuration("PUBLISH_RETRY_INTERVAL", 5*time.Second)

	// local spool (disk cache)
	cfg.SpoolDir = getEnv("SPOOL_DIR", "/tmp/udp-logger-spool")
	cfg.SpoolMaxBytes = getEnvInt64("SPOOL_MAX_BYTES", 1024*1024*1024)      // 1GiB
	cfg.SpoolSegmentBytes = getEnvInt64("SPOOL_SEGMENT_BYTES", 16*1024*1024) // 16MiB
	cfg.SpoolFsync = getEnvBool("SPOOL_FSYNC", false)
	cfg.SpoolLogInterval = getEnvDuration("SPOOL_LOG_INTERVAL", 30*time.Second)

	cfg.Rabbit.Host = getEnv("RABBITMQ_HOST", "localhost")
	cfg.Rabbit.Port = getEnvInt("RABBITMQ_PORT", 5672)
	cfg.Rabbit.User = getEnv("RABBITMQ_USER", "guest")
	cfg.Rabbit.Password = getEnv("RABBITMQ_PASSWORD", "guest")
	cfg.Rabbit.VHost = getEnv("RABBITMQ_VHOST", "/")

	// TLS: if RABBITMQ_TLS explicitly set -> respect it; else auto-enable on 5671.
	_, tlsVal, tlsSet := getEnvBoolMaybe("RABBITMQ_TLS")
	if tlsSet {
		cfg.Rabbit.TLS.Enabled = tlsVal
	} else {
		cfg.Rabbit.TLS.Enabled = cfg.Rabbit.Port == 5671
	}

	certsDir := getEnv("CERTS", "")
	def := func(p string) string {
		if certsDir == "" {
			return ""
		}
		// Python example expects CERTS to contain trailing slash; we make it robust.
		if strings.HasSuffix(certsDir, "/") {
			return certsDir + p
		}
		return certsDir + "/" + p
	}

	cfg.Rabbit.TLS.CAFile = getEnv("RABBITMQ_CA_FILE", def("ca.pem"))
	cfg.Rabbit.TLS.CertFile = getEnv("RABBITMQ_CERT_FILE", def("tls.crt"))
	cfg.Rabbit.TLS.KeyFile = getEnv("RABBITMQ_KEY_FILE", def("tls.key"))
	cfg.Rabbit.TLS.ServerName = getEnv("RABBITMQ_TLS_SERVER_NAME", "")
	cfg.Rabbit.TLS.InsecureSkipVerify = getEnvBool("RABBITMQ_TLS_INSECURE_SKIP_VERIFY", false)

	// Cluster mode configuration
	cfg.Cluster.Mode = strings.ToLower(getEnv("CLUSTER_MODE", ""))
	cfg.Cluster.MasterAddr = getEnv("MASTER_ADDR", "")
	cfg.Cluster.MasterPort = getEnvInt("MASTER_PORT", 9999)
	
	// If MASTER_ADDR is set, automatically enable client mode
	if cfg.Cluster.Mode == "" && cfg.Cluster.MasterAddr != "" {
		cfg.Cluster.Mode = "client"
	}
	
	// Cluster TLS (for encryption between client and master)
	cfg.Cluster.TLS.Enabled = getEnvBool("CLUSTER_TLS", true)
	// CA file: if CLUSTER_CA_FILE explicitly set, use it; else try default from CERTS dir
	cfg.Cluster.TLS.CAFile = getEnv("CLUSTER_CA_FILE", def("ca.pem"))
	// Client cert/key: only use defaults if explicitly set via CERTS, otherwise empty (allows CA-only mode)
	cfg.Cluster.TLS.CertFile = getEnv("CLUSTER_CERT_FILE", "")
	if cfg.Cluster.TLS.CertFile == "" && certsDir != "" {
		cfg.Cluster.TLS.CertFile = def("tls.crt")
	}
	cfg.Cluster.TLS.KeyFile = getEnv("CLUSTER_KEY_FILE", "")
	if cfg.Cluster.TLS.KeyFile == "" && certsDir != "" {
		cfg.Cluster.TLS.KeyFile = def("tls.key")
	}
	cfg.Cluster.TLS.ServerName = getEnv("CLUSTER_TLS_SERVER_NAME", "")
	cfg.Cluster.TLS.InsecureSkipVerify = getEnvBool("CLUSTER_TLS_INSECURE_SKIP_VERIFY", false)

	if cfg.BufferSize <= 0 {
		return Config{}, errors.New("BUFFER_SIZE must be > 0")
	}
	if cfg.UDPReadBufBytes <= 0 {
		return Config{}, errors.New("UDP_READ_BUFFER must be > 0")
	}
	if cfg.PublishRetryInterval <= 0 {
		return Config{}, errors.New("PUBLISH_RETRY_INTERVAL must be > 0")
	}
	if cfg.SpoolMaxBytes < 0 {
		return Config{}, errors.New("SPOOL_MAX_BYTES must be >= 0")
	}
	if cfg.SpoolSegmentBytes <= 0 {
		return Config{}, errors.New("SPOOL_SEGMENT_BYTES must be > 0")
	}
	if cfg.SpoolLogInterval < 0 {
		return Config{}, errors.New("SPOOL_LOG_INTERVAL must be >= 0")
	}

	return cfg, nil
}

func (r RabbitConfig) AMQPURI() string {
	vhostEscaped := escapeVHost(r.VHost)
	scheme := "amqp"
	if r.TLS.Enabled {
		scheme = "amqps"
	}
	userInfo := url.UserPassword(r.User, r.Password).String()
	return fmt.Sprintf("%s://%s@%s:%d/%s", scheme, userInfo, r.Host, r.Port, vhostEscaped)
}

func escapeVHost(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || v == "/" {
		return "%2F"
	}
	v = strings.TrimPrefix(v, "/")
	if v == "" {
		return "%2F"
	}
	return url.PathEscape(v)
}

func (r RabbitConfig) TLSConfig() (*tls.Config, error) {
	if !r.TLS.Enabled {
		return nil, nil
	}
	return buildTLSConfig(r.TLS)
}

func (c ClusterConfig) TLSConfig() (*tls.Config, error) {
	if !c.TLS.Enabled {
		return nil, nil
	}
	return buildTLSConfig(c.TLS)
}

func getEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func getEnvInt64(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return i
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	// support both "5s" and "5" (seconds)
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if i, err := strconv.Atoi(v); err == nil && i > 0 {
		return time.Duration(i) * time.Second
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	_, b, ok := getEnvBoolMaybe(key)
	if !ok {
		return def
	}
	return b
}

func getEnvBoolMaybe(key string) (raw string, val bool, ok bool) {
	raw = strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return "", false, false
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "y", "on":
		return raw, true, true
	case "0", "false", "no", "n", "off":
		return raw, false, true
	default:
		return raw, false, false
	}
}


