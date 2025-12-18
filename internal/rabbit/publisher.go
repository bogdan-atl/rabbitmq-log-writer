package rabbit

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"rabbit-log-writer/internal/config"
	"rabbit-log-writer/internal/metrics"
	"rabbit-log-writer/internal/spool"
)

type Publisher struct {
	Config            config.RabbitConfig
	QueueName         string
	PublishInterval   time.Duration
	DropLogEvery      uint64
	TimestampLocation *time.Location
	Metrics           *metrics.Metrics
}

func (p Publisher) Run(ctx context.Context, s *spool.Spool) error {
	if s == nil {
		return errors.New("rabbit publisher: spool is nil")
	}
	if p.QueueName == "" {
		return errors.New("rabbit publisher: QueueName is empty")
	}
	if p.PublishInterval <= 0 {
		p.PublishInterval = 5 * time.Second
	}
	if p.DropLogEvery == 0 {
		p.DropLogEvery = 1000
	}
	if p.TimestampLocation == nil {
		p.TimestampLocation = time.Local
	}

	var (
		conn     *amqp.Connection
		ch       *amqp.Channel
		connDone chan *amqp.Error
		chDone   chan *amqp.Error
	)

	closeAll := func() {
		if p.Metrics != nil {
			p.Metrics.RabbitConnected.Set(0)
		}
		if ch != nil {
			_ = ch.Close()
			ch = nil
		}
		if conn != nil {
			_ = conn.Close()
			conn = nil
		}
		connDone, chDone = nil, nil
	}
	defer closeAll()

	connect := func() error {
		closeAll()

		uri := p.Config.AMQPURI()
		tlsCfg, err := p.Config.TLSConfig()
		if err != nil {
			if p.Metrics != nil {
				p.Metrics.RabbitConnectErrorsTotal.Inc()
			}
			return err
		}

		var c *amqp.Connection
		if tlsCfg != nil {
			c, err = amqp.DialTLS(uri, tlsCfg)
		} else {
			c, err = amqp.Dial(uri)
		}
		if err != nil {
			return err
		}

		channel, err := c.Channel()
		if err != nil {
			_ = c.Close()
			if p.Metrics != nil {
				p.Metrics.RabbitConnectErrorsTotal.Inc()
			}
			return err
		}

		_, err = channel.QueueDeclare(
			p.QueueName,
			true,  // durable
			false, // autoDelete
			false, // exclusive
			false, // noWait
			nil,   // args
		)
		if err != nil {
			_ = channel.Close()
			_ = c.Close()
			if p.Metrics != nil {
				p.Metrics.RabbitConnectErrorsTotal.Inc()
			}
			return err
		}

		conn = c
		ch = channel
		connDone = conn.NotifyClose(make(chan *amqp.Error, 1))
		chDone = ch.NotifyClose(make(chan *amqp.Error, 1))
		if p.Metrics != nil {
			p.Metrics.RabbitConnected.Set(1)
		}

		log.Printf("rabbit connected: %s:%d vhost=%s tls=%v queue=%s",
			p.Config.Host, p.Config.Port, p.Config.VHost, p.Config.TLS.Enabled, p.QueueName)
		return nil
	}

	ensureConnected := func() bool {
		if conn == nil || ch == nil {
			return false
		}
		select {
		case <-connDone:
			return false
		case <-chDone:
			return false
		default:
			return true
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, ack, err := s.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			// keep looping; spool errors are unusual but shouldn't crash the pod forever
			log.Printf("spool next error: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}

		// Retry publish of THIS message until success or context cancelled.
		for {
			for !ensureConnected() {
				if err := connect(); err != nil {
					log.Printf("rabbit connect error: %v; retry in %s", err, p.PublishInterval)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(p.PublishInterval):
						continue
					}
				}
			}

			body := withTimestamp(msg, p.TimestampLocation)
			err := ch.PublishWithContext(
				ctx,
				"",          // default exchange
				p.QueueName, // routing key = queue name
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType:  "text/plain",
					DeliveryMode: amqp.Persistent,
					Body:         []byte(body),
					Timestamp:    time.Now(),
				},
			)
			if err == nil {
				if p.Metrics != nil {
					p.Metrics.RabbitPublishedTotal.Inc()
				}
				if ack != nil {
					if err := ack(); err != nil {
						log.Printf("spool ack error: %v", err)
					}
				}
				break
			}

			if p.Metrics != nil {
				p.Metrics.RabbitPublishErrorsTotal.Inc()
			}
			log.Printf("rabbit publish error: %v; reconnect and retry in %s", err, p.PublishInterval)
			closeAll()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.PublishInterval):
			}
		}
	}
}

func withTimestamp(msg string, loc *time.Location) string {
	now := time.Now()
	if loc != nil {
		now = now.In(loc)
	}
	ts := now.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("[%s] %s", ts, msg)
}


