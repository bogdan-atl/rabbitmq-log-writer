package queue

import "context"

type Stats struct {
	Queued     int64
	Bytes      int64
	ReadSeg    int
	WriteSeg   int
	ReadOffset int64
	Dropped    int64
	Processing int64
	DeadLetter int64
	Requeued   int64
	LastProblemUnix int64
}

type Queue interface {
	Enqueue(msg string) error
	Next(ctx context.Context) (string, func() error, error)
	Stats() Stats
	Close() error
}
