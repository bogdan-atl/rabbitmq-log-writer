package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client         *redis.Client
	listKey        string
	processingKey  string
	deadLetterKey  string
	maxRetries     int
	visibilityTTL  time.Duration
	reaperInterval time.Duration
	stopReaper     context.CancelFunc

	queued          int64
	bytes           int64
	processing      int64
	deadLetter      int64
	requeued        int64
	lastProblemUnix int64
	idSeq           uint64
}

type redisMessage struct {
	ID              string `json:"id"`
	Body            string `json:"body"`
	Attempts        int    `json:"attempts"`
	LastAttemptUnix int64  `json:"last_attempt_unix"`
	CreatedUnix     int64  `json:"created_unix"`
}

func NewRedisQueue(
	ctx context.Context,
	addr, password, listKey, processingKey, deadLetterKey string,
	db int,
	maxRetries int,
	visibilityTTL, reaperInterval time.Duration,
) (*RedisQueue, error) {
	if addr == "" {
		return nil, errors.New("redis queue: addr is empty")
	}
	if listKey == "" {
		listKey = "udp-logger:queue"
	}
	if processingKey == "" {
		processingKey = listKey + ":processing"
	}
	if deadLetterKey == "" {
		deadLetterKey = listKey + ":dead-letter"
	}
	if maxRetries <= 0 {
		maxRetries = 10
	}
	if visibilityTTL <= 0 {
		visibilityTTL = 30 * time.Second
	}
	if reaperInterval <= 0 {
		reaperInterval = 5 * time.Second
	}

	c := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	lLen, err := c.LLen(ctx, listKey).Result()
	if err != nil {
		return nil, err
	}
	pLen, err := c.LLen(ctx, processingKey).Result()
	if err != nil {
		return nil, err
	}
	dlLen, err := c.LLen(ctx, deadLetterKey).Result()
	if err != nil {
		return nil, err
	}

	reaperCtx, cancel := context.WithCancel(context.Background())

	q := &RedisQueue{
		client:         c,
		listKey:        listKey,
		processingKey:  processingKey,
		deadLetterKey:  deadLetterKey,
		maxRetries:     maxRetries,
		visibilityTTL:  visibilityTTL,
		reaperInterval: reaperInterval,
		stopReaper:     cancel,
		queued:         lLen + pLen,
		processing:     pLen,
		deadLetter:     dlLen,
	}
	go q.runReaper(reaperCtx)
	return q, nil
}

func (q *RedisQueue) Enqueue(msg string) error {
	now := time.Now().Unix()
	env := redisMessage{
		ID:          q.newMessageID(),
		Body:        msg,
		CreatedUnix: now,
	}
	encoded, err := encodeMessage(env)
	if err != nil {
		return err
	}
	if err := q.client.LPush(context.Background(), q.listKey, encoded).Err(); err != nil {
		return err
	}
	atomic.AddInt64(&q.queued, 1)
	atomic.AddInt64(&q.bytes, int64(len(msg)))
	return nil
}

func (q *RedisQueue) Next(ctx context.Context) (string, func() error, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}

		// BRPOPLPUSH gives at-least-once semantics: message is moved
		// to processing and removed from there only on ack.
		raw, err := q.client.BRPopLPush(ctx, q.listKey, q.processingKey, time.Second).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return "", nil, ctx.Err()
			}
			return "", nil, err
		}

		env := decodeMessage(raw)
		env.Attempts++
		env.LastAttemptUnix = time.Now().Unix()
		updatedRaw, err := encodeMessage(env)
		if err != nil {
			updatedRaw = raw
		}

		if updatedRaw != raw {
			removed, remErr := q.client.LRem(ctx, q.processingKey, 1, raw).Result()
			if remErr != nil {
				return "", nil, remErr
			}
			if removed > 0 {
				if err := q.client.LPush(ctx, q.processingKey, updatedRaw).Err(); err != nil {
					return "", nil, err
				}
			}
		}

		atomic.AddInt64(&q.processing, 1)
		bodyLen := int64(len(env.Body))
		var once sync.Once
		ack := func() error {
			var ackErr error
			once.Do(func() {
				removed, err := q.client.LRem(context.Background(), q.processingKey, 1, updatedRaw).Result()
				if err != nil {
					ackErr = err
					return
				}
				if removed > 0 {
					atomic.AddInt64(&q.queued, -1)
					atomic.AddInt64(&q.processing, -1)
					q.subBytes(bodyLen)
				}
			})
			return ackErr
		}
		return env.Body, ack, nil
	}
}

func (q *RedisQueue) Stats() Stats {
	return Stats{
		Queued:          atomic.LoadInt64(&q.queued),
		Bytes:           atomic.LoadInt64(&q.bytes),
		Processing:      atomic.LoadInt64(&q.processing),
		DeadLetter:      atomic.LoadInt64(&q.deadLetter),
		Requeued:        atomic.LoadInt64(&q.requeued),
		LastProblemUnix: atomic.LoadInt64(&q.lastProblemUnix),
	}
}

func (q *RedisQueue) Close() error {
	if q.stopReaper != nil {
		q.stopReaper()
	}
	return q.client.Close()
}

func (q *RedisQueue) runReaper(ctx context.Context) {
	t := time.NewTicker(q.reaperInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			q.reapProcessing(ctx)
		}
	}
}

func (q *RedisQueue) reapProcessing(ctx context.Context) {
	if pLen, err := q.client.LLen(ctx, q.processingKey).Result(); err == nil {
		atomic.StoreInt64(&q.processing, pLen)
	}
	items, err := q.client.LRange(ctx, q.processingKey, 0, -1).Result()
	if err != nil {
		return
	}
	now := time.Now().Unix()
	ttlSec := int64(q.visibilityTTL / time.Second)
	if ttlSec <= 0 {
		ttlSec = 1
	}

	for _, raw := range items {
		env := decodeMessage(raw)
		if env.LastAttemptUnix == 0 || now-env.LastAttemptUnix < ttlSec {
			continue
		}

		removed, remErr := q.client.LRem(ctx, q.processingKey, 1, raw).Result()
		if remErr != nil || removed == 0 {
			continue
		}

		atomic.StoreInt64(&q.lastProblemUnix, now)
		atomic.AddInt64(&q.processing, -1)

		if env.Attempts >= q.maxRetries {
			if err := q.client.LPush(ctx, q.deadLetterKey, raw).Err(); err != nil {
				// Best effort rollback to main queue if dead-letter push failed.
				_ = q.client.LPush(ctx, q.listKey, raw).Err()
				continue
			}
			atomic.AddInt64(&q.queued, -1)
			atomic.AddInt64(&q.deadLetter, 1)
			q.subBytes(int64(len(env.Body)))
			continue
		}

		if err := q.client.LPush(ctx, q.listKey, raw).Err(); err != nil {
			// Keep message alive by putting it back into processing if possible.
			_ = q.client.LPush(ctx, q.processingKey, raw).Err()
			atomic.AddInt64(&q.processing, 1)
			continue
		}
		atomic.AddInt64(&q.requeued, 1)
	}
}

func (q *RedisQueue) subBytes(delta int64) {
	if delta <= 0 {
		return
	}
	for {
		cur := atomic.LoadInt64(&q.bytes)
		next := cur - delta
		if next < 0 {
			next = 0
		}
		if atomic.CompareAndSwapInt64(&q.bytes, cur, next) {
			return
		}
	}
}

func (q *RedisQueue) newMessageID() string {
	seq := atomic.AddUint64(&q.idSeq, 1)
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), seq)
}

func encodeMessage(m redisMessage) (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeMessage(raw string) redisMessage {
	var m redisMessage
	if err := json.Unmarshal([]byte(raw), &m); err == nil && m.Body != "" {
		if m.ID == "" {
			m.ID = fmt.Sprintf("legacy-%d", time.Now().UnixNano())
		}
		return m
	}
	return redisMessage{
		ID:          fmt.Sprintf("legacy-%d", time.Now().UnixNano()),
		Body:        raw,
		Attempts:    0,
		CreatedUnix: time.Now().Unix(),
	}
}
