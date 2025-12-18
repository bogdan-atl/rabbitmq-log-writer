package spool

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrClosed   = errors.New("spool: closed")
	ErrTooLarge = errors.New("spool: record larger than max bytes")
)

// Spool is a simple file-backed FIFO queue:
// - append-only segment files: seg-000000.dat, seg-000001.dat, ...
// - meta.json stores read position (segment + offset)
// Messages are stored as: 4-byte big-endian length + bytes.
type Spool struct {
	mu sync.Mutex

	dir            string
	maxBytes       int64
	segmentMaxBytes int64
	fsync          bool

	notify chan struct{}
	closed bool

	// positions
	readSeg    int
	readOffset int64
	writeSeg   int

	// open files
	readFile  *os.File
	writeFile *os.File

	// total size of all segment files (best-effort tracked)
	totalBytes int64

	// number of records currently pending in spool (best-effort, kept accurate for this implementation)
	queued int64

	// number of records dropped due to maxBytes (drop-oldest policy)
	dropped int64
}

type meta struct {
	ReadSeg    int   `json:"read_seg"`
	ReadOffset int64 `json:"read_offset"`
}

func Open(dir string, maxBytes, segmentMaxBytes int64, fsync bool) (*Spool, error) {
	if dir == "" {
		return nil, fmt.Errorf("spool: dir is empty")
	}
	if segmentMaxBytes <= 0 {
		return nil, fmt.Errorf("spool: segmentMaxBytes must be > 0")
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	s := &Spool{
		dir:             dir,
		maxBytes:        maxBytes,
		segmentMaxBytes: segmentMaxBytes,
		fsync:           fsync,
		notify:          make(chan struct{}, 1),
	}

	if err := s.loadMeta(); err != nil {
		return nil, err
	}

	if err := s.discoverSegments(); err != nil {
		return nil, err
	}

	// Ensure files are opened
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureWriteFileLocked(); err != nil {
		return nil, err
	}
	if err := s.ensureReadFileLocked(); err != nil {
		return nil, err
	}

	// compute queued count on startup (so we can report it via logs/metrics)
	if err := s.recountQueuedLocked(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.readFile != nil {
		_ = s.readFile.Close()
		s.readFile = nil
	}
	if s.writeFile != nil {
		_ = s.writeFile.Close()
		s.writeFile = nil
	}
	s.signalLocked()
	return nil
}

func (s *Spool) Enqueue(msg string) error {
	b := []byte(msg)
	recBytes := int64(4 + len(b))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if s.maxBytes > 0 && recBytes > s.maxBytes {
		return ErrTooLarge
	}
	if err := s.ensureWriteFileLocked(); err != nil {
		return err
	}

	// rotate segment if needed
	if err := s.rotateIfNeededLocked(recBytes); err != nil {
		return err
	}

	// enforce maxBytes by dropping oldest segments/records (best-effort, drop-oldest policy)
	if s.maxBytes > 0 {
		for s.totalBytes+recBytes > s.maxBytes {
			if err := s.dropOldestLocked(); err != nil {
				// if we can't free space, we must drop this record
				return err
			}
		}
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := s.writeFile.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := s.writeFile.Write(b); err != nil {
		return err
	}
	s.totalBytes += recBytes
	s.queued++
	if s.fsync {
		_ = s.writeFile.Sync()
	}
	s.signalLocked()
	return nil
}

type Stats struct {
	Queued     int64
	Bytes      int64
	ReadSeg    int
	WriteSeg   int
	ReadOffset int64
	Dropped    int64
}

func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Queued:     s.queued,
		Bytes:      s.totalBytes,
		ReadSeg:    s.readSeg,
		WriteSeg:   s.writeSeg,
		ReadOffset: s.readOffset,
		Dropped:    s.dropped,
	}
}

// Next blocks until a message is available (or ctx is cancelled), then returns the message and an Ack function.
// Ack advances the read cursor and may delete fully-consumed segments.
func (s *Spool) Next(ctx context.Context) (string, func() error, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return "", nil, ErrClosed
		}

		msg, ack, ok, err := s.peekLocked()
		s.mu.Unlock()

		if err != nil {
			return "", nil, err
		}
		if ok {
			return msg, ack, nil
		}

		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()
		case <-s.notify:
			continue
		}
	}
}

func (s *Spool) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Spool) metaPath() string { return filepath.Join(s.dir, "meta.json") }

func (s *Spool) loadMeta() error {
	p := s.metaPath()
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var m meta
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	s.readSeg = m.ReadSeg
	s.readOffset = m.ReadOffset
	return nil
}

func (s *Spool) saveMetaLocked() error {
	m := meta{
		ReadSeg:    s.readSeg,
		ReadOffset: s.readOffset,
	}
	b, err := json.Marshal(&m)
	if err != nil {
		return err
	}
	tmp := s.metaPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.metaPath())
}

func (s *Spool) segmentPath(seg int) string {
	return filepath.Join(s.dir, fmt.Sprintf("seg-%06d.dat", seg))
}

func (s *Spool) discoverSegments() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var segs []int
	var total int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".dat") {
			continue
		}
		n := strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), ".dat")
		i, err := strconv.Atoi(n)
		if err != nil {
			continue
		}
		segs = append(segs, i)
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	sort.Ints(segs)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalBytes = total
	if len(segs) == 0 {
		// start at current readSeg
		s.writeSeg = s.readSeg
		return nil
	}
	// ensure readSeg is not behind first existing segment
	if s.readSeg < segs[0] {
		s.readSeg = segs[0]
		s.readOffset = 0
	}
	s.writeSeg = segs[len(segs)-1]
	return nil
}

func (s *Spool) ensureWriteFileLocked() error {
	if s.writeFile != nil {
		return nil
	}
	p := s.segmentPath(s.writeSeg)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.writeFile = f
	return nil
}

func (s *Spool) ensureReadFileLocked() error {
	if s.readFile != nil {
		return nil
	}
	p := s.segmentPath(s.readSeg)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return err
	}
	s.readFile = f
	return nil
}

func (s *Spool) rotateIfNeededLocked(extra int64) error {
	// check current segment size
	if s.writeFile == nil {
		return nil
	}
	info, err := s.writeFile.Stat()
	if err != nil {
		return err
	}
	if info.Size()+extra <= s.segmentMaxBytes {
		return nil
	}
	_ = s.writeFile.Close()
	s.writeFile = nil
	s.writeSeg++
	return s.ensureWriteFileLocked()
}

func (s *Spool) peekLocked() (msg string, ack func() error, ok bool, err error) {
	if err := s.ensureReadFileLocked(); err != nil {
		return "", nil, false, err
	}

	// Determine end position for current read segment.
	// If readSeg < writeSeg -> end = file size.
	// If readSeg == writeSeg -> end = current write file size (append-only).
	end, err := s.segmentEndLocked()
	if err != nil {
		return "", nil, false, err
	}
	if s.readOffset >= end {
		// if there are later segments, move to next
		if s.readSeg < s.writeSeg {
			if err := s.advanceToNextSegmentLocked(); err != nil {
				return "", nil, false, err
			}
			// retry after advancing
			return s.peekLocked()
		}
		return "", nil, false, nil
	}

	var lenBuf [4]byte
	if _, err := s.readAtLocked(lenBuf[:], s.readSeg, s.readOffset); err != nil {
		if errors.Is(err, io.EOF) {
			return "", nil, false, nil
		}
		return "", nil, false, err
	}
	l := int64(binary.BigEndian.Uint32(lenBuf[:]))
	if l < 0 {
		return "", nil, false, fmt.Errorf("spool: negative length")
	}
	if s.readOffset+4+l > end {
		// record not fully written yet
		return "", nil, false, nil
	}

	body := make([]byte, l)
	if _, err := s.readAtLocked(body, s.readSeg, s.readOffset+4); err != nil {
		if errors.Is(err, io.EOF) {
			return "", nil, false, nil
		}
		return "", nil, false, err
	}

	nextOffset := s.readOffset + 4 + l
	curSeg := s.readSeg

	ack = func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed {
			return ErrClosed
		}
		// only ack if we are still at the same position
		if s.readSeg != curSeg || s.readOffset != (nextOffset-4-l) {
			return nil
		}
		// record size: 4-byte length header + message body
		recBytes := int64(4 + l)
		s.readOffset = nextOffset
		if s.queued > 0 {
			s.queued--
		}
		// decrease totalBytes to reflect consumed message (more accurate than waiting for segment deletion)
		if s.totalBytes >= recBytes {
			s.totalBytes -= recBytes
		} else {
			// defensive: shouldn't happen, but if it does, reset to 0 rather than negative
			s.totalBytes = 0
		}
		if err := s.saveMetaLocked(); err != nil {
			return err
		}

		// if we've fully consumed a segment and there are later ones, delete it
		end, err := s.segmentEndLocked()
		if err != nil {
			return err
		}
		if s.readOffset >= end && s.readSeg < s.writeSeg {
			if err := s.advanceToNextSegmentLocked(); err != nil {
				return err
			}
		}
		s.signalLocked()
		return nil
	}

	return string(body), ack, true, nil
}

func (s *Spool) segmentEndLocked() (int64, error) {
	// if readSeg == writeSeg and writeFile is open, stat it (same path) to see latest size
	p := s.segmentPath(s.readSeg)
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func (s *Spool) readAtLocked(dst []byte, seg int, off int64) (int, error) {
	if seg != s.readSeg {
		return 0, fmt.Errorf("spool: invalid seg read")
	}
	if s.readFile == nil {
		return 0, fmt.Errorf("spool: readFile nil")
	}
	return s.readFile.ReadAt(dst, off)
}

func (s *Spool) advanceToNextSegmentLocked() error {
	// close current read file
	if s.readFile != nil {
		_ = s.readFile.Close()
		s.readFile = nil
	}
	// delete fully consumed segment file
	// Note: we don't decrease totalBytes here because all messages in this segment
	// have already been acked (and totalBytes was decreased during each ack).
	oldPath := s.segmentPath(s.readSeg)
	_ = os.Remove(oldPath)

	s.readSeg++
	s.readOffset = 0
	if err := s.saveMetaLocked(); err != nil {
		return err
	}
	return s.ensureReadFileLocked()
}

func (s *Spool) dropOldestLocked() error {
	// Drop-oldest policy:
	// - if readSeg < writeSeg: delete readSeg and advance
	// - else (only one segment): truncate it by resetting offsets (lose everything), start fresh
	if s.readSeg < s.writeSeg {
		// drop remaining records in current read segment (from readOffset to end)
		if dropped, err := countRecordsRemaining(s.segmentPath(s.readSeg), s.readOffset); err == nil && dropped > 0 {
			s.queued -= dropped
			if s.queued < 0 {
				s.queued = 0
			}
			s.dropped += dropped
		}
		return s.advanceToNextSegmentLocked()
	}
	// same segment: drop all content
	p := s.segmentPath(s.readSeg)
	if dropped, err := countRecordsRemaining(p, s.readOffset); err == nil && dropped > 0 {
		s.queued -= dropped
		if s.queued < 0 {
			s.queued = 0
		}
		s.dropped += dropped
	}
	if s.readFile != nil {
		_ = s.readFile.Close()
		s.readFile = nil
	}
	if s.writeFile != nil {
		_ = s.writeFile.Close()
		s.writeFile = nil
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	s.totalBytes = 0
	s.readOffset = 0
	// keep same seg number; recreate files
	if err := s.ensureWriteFileLocked(); err != nil {
		return err
	}
	if err := s.ensureReadFileLocked(); err != nil {
		return err
	}
	return s.saveMetaLocked()
}

func (s *Spool) recountQueuedLocked() error {
	// count records from cursor to end (best-effort)
	var total int64
	for seg := s.readSeg; seg <= s.writeSeg; seg++ {
		start := int64(0)
		if seg == s.readSeg {
			start = s.readOffset
		}
		n, err := countRecordsRemaining(s.segmentPath(seg), start)
		if err != nil {
			// If a segment doesn't exist, skip it.
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		total += n
	}
	s.queued = total
	return nil
}

func countRecordsRemaining(path string, startOffset int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	if startOffset > 0 {
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			return 0, err
		}
	}

	var cnt int64
	var lenBuf [4]byte
	for {
		_, err := io.ReadFull(f, lenBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return cnt, nil
			}
			return cnt, err
		}
		l := int64(binary.BigEndian.Uint32(lenBuf[:]))
		if l < 0 {
			return cnt, fmt.Errorf("spool: invalid record length")
		}
		if _, err := f.Seek(l, io.SeekCurrent); err != nil {
			if errors.Is(err, io.EOF) {
				return cnt, nil
			}
			return cnt, err
		}
		cnt++
	}
}



