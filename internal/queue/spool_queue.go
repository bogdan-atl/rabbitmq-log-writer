package queue

import (
	"context"

	"rabbit-log-writer/internal/spool"
)

type SpoolQueue struct {
	sp *spool.Spool
}

func NewSpoolQueue(sp *spool.Spool) *SpoolQueue {
	return &SpoolQueue{sp: sp}
}

func (q *SpoolQueue) Enqueue(msg string) error {
	return q.sp.Enqueue(msg)
}

func (q *SpoolQueue) Next(ctx context.Context) (string, func() error, error) {
	return q.sp.Next(ctx)
}

func (q *SpoolQueue) Stats() Stats {
	st := q.sp.Stats()
	return Stats{
		Queued:     st.Queued,
		Bytes:      st.Bytes,
		ReadSeg:    st.ReadSeg,
		WriteSeg:   st.WriteSeg,
		ReadOffset: st.ReadOffset,
		Dropped:    st.Dropped,
	}
}

func (q *SpoolQueue) Close() error {
	return q.sp.Close()
}
