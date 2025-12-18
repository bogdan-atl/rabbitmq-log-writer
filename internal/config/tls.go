package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func buildTLSConfig(c RabbitTLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.InsecureSkipVerify,
		ServerName:         c.ServerName,
	}

	// Client cert (mTLS)
	if c.CertFile != "" || c.KeyFile != "" {
		if c.CertFile == "" || c.KeyFile == "" {
			return nil, fmt.Errorf("both cert and key must be provided (RABBITMQ_CERT_FILE/RABBITMQ_KEY_FILE)")
		}
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	// Custom CA
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid CA pem: %s", c.CAFile)
		}
		tlsCfg.RootCAs = pool
	}

	return tlsCfg, nil
}



