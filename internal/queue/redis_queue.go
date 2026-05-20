package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client   *redis.Client
	listKey  string
	ackedKey string

	queued int64
	bytes  int64
}

func NewRedisQueue(ctx context.Context, addr, password, listKey, ackedKey string, db int) (*RedisQueue, error) {
	if addr == "" {
		return nil, errors.New("redis queue: addr is empty")
	}
	if listKey == "" {
		listKey = "udp-logger:queue"
	}
	if ackedKey == "" {
		ackedKey = listKey + ":processing"
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
	aLen, err := c.LLen(ctx, ackedKey).Result()
	if err != nil {
		return nil, err
	}

	return &RedisQueue{
		client:   c,
		listKey:  listKey,
		ackedKey: ackedKey,
		queued:   lLen + aLen,
	}, nil
}

func (q *RedisQueue) Enqueue(msg string) error {
	if err := q.client.LPush(context.Background(), q.listKey, msg).Err(); err != nil {
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
		// to processing list and removed from there only on ack.
		msg, err := q.client.BRPopLPush(ctx, q.listKey, q.ackedKey, time.Second).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return "", nil, ctx.Err()
			}
			return "", nil, err
		}

		var once sync.Once
		ack := func() error {
			var ackErr error
			once.Do(func() {
				removed, err := q.client.LRem(context.Background(), q.ackedKey, 1, msg).Result()
				if err != nil {
					ackErr = err
					return
				}
				if removed > 0 {
					atomic.AddInt64(&q.queued, -1)
					atomic.AddInt64(&q.bytes, -int64(len(msg)))
				}
			})
			return ackErr
		}
		return msg, ack, nil
	}
}

func (q *RedisQueue) Stats() Stats {
	return Stats{
		Queued: atomic.LoadInt64(&q.queued),
		Bytes:  atomic.LoadInt64(&q.bytes),
	}
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}
