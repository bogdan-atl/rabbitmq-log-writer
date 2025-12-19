package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type tlsConfigSource interface {
	GetCAFile() string
	GetCertFile() string
	GetKeyFile() string
	GetServerName() string
	GetInsecureSkipVerify() bool
}

func (r RabbitTLSConfig) GetCAFile() string             { return r.CAFile }
func (r RabbitTLSConfig) GetCertFile() string           { return r.CertFile }
func (r RabbitTLSConfig) GetKeyFile() string            { return r.KeyFile }
func (r RabbitTLSConfig) GetServerName() string         { return r.ServerName }
func (r RabbitTLSConfig) GetInsecureSkipVerify() bool   { return r.InsecureSkipVerify }

func (c ClusterTLSConfig) GetCAFile() string             { return c.CAFile }
func (c ClusterTLSConfig) GetCertFile() string           { return c.CertFile }
func (c ClusterTLSConfig) GetKeyFile() string             { return c.KeyFile }
func (c ClusterTLSConfig) GetServerName() string         { return c.ServerName }
func (c ClusterTLSConfig) GetInsecureSkipVerify() bool   { return c.InsecureSkipVerify }

func buildTLSConfig(c tlsConfigSource) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.GetInsecureSkipVerify(),
		ServerName:         c.GetServerName(),
	}

	// Client cert (mTLS)
	if c.GetCertFile() != "" || c.GetKeyFile() != "" {
		if c.GetCertFile() == "" || c.GetKeyFile() == "" {
			return nil, fmt.Errorf("both cert and key must be provided")
		}
		cert, err := tls.LoadX509KeyPair(c.GetCertFile(), c.GetKeyFile())
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	// Custom CA
	if c.GetCAFile() != "" {
		pem, err := os.ReadFile(c.GetCAFile())
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid CA pem: %s", c.GetCAFile())
		}
		tlsCfg.RootCAs = pool
	}

	return tlsCfg, nil
}




