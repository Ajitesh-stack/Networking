// Package wal implements a write-ahead log (WAL) for persistent transaction logging.
//
// Binary Entry Layout (Big-Endian):
// [ CRC32 Checksum : 4 bytes ] [ Sequence Number : 8 bytes ] [ Payload Length : 4 bytes ] [ Payload Bytes : N bytes ]
//
// - CRC32 Checksum (4 bytes): IEEE CRC32 checksum computed over the concatenation of the Sequence Number (8 bytes), Payload Length (4 bytes), and the Payload Bytes (N bytes).
// - Sequence Number (8 bytes): Monotonically increasing unique sequence ID (uint64).
// - Payload Length (4 bytes): Length prefix indicating the size of the telemetry payload (uint32).
// - Payload Bytes (N bytes): Raw telemetry packet string.
package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"

	"github.com/Ajitesh-stack/spatial-ingestion-server/cache"
)

// WAL manages append-only logging of telemetry payloads to a log file on disk.
type WAL struct {
	file        *os.File
	mu          sync.Mutex
	sequence    uint64
	currentSize int64
	syncEvery   int // flush to disk every N writes
	writeCount  int // counts writes since last sync
	path        string
}

// New opens or creates a WAL file at the given path in append-only mode.
func New(path string, syncEvery int) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if syncEvery <= 0 {
		syncEvery = 1
	}

	return &WAL{
		file:        file,
		currentSize: stat.Size(),
		syncEvery:   syncEvery,
		path:        path,
	}, nil
}

// Write writes the telemetry payload string to the log file.
// It is thread-safe and writes the entry in a single atomic Write call.
func (w *WAL) Write(payload string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sequence++
	seq := w.sequence

	payloadBytes := []byte(payload)
	payloadLen := uint32(len(payloadBytes))

	entrySize := 16 + int64(payloadLen)
	buf := make([]byte, entrySize)

	// Format layout in buffer:
	// buf[0:4]   -> CRC32 (filled later)
	// buf[4:12]  -> Sequence (8 bytes)
	// buf[12:16] -> Length (4 bytes)
	// buf[16:]   -> Payload (N bytes)
	binary.BigEndian.PutUint64(buf[4:12], seq)
	binary.BigEndian.PutUint32(buf[12:16], payloadLen)
	copy(buf[16:], payloadBytes)

	// CRC32 checksum is calculated over sequence + length + payload (buf[4:])
	checksum := crc32.Checksum(buf[4:], crc32.IEEETable)
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	// Write buffer in a single call to prevent interleaving of concurrent writes
	n, err := w.file.Write(buf)
	if err != nil {
		return err
	}
	if int64(n) < entrySize {
		w.currentSize += int64(n)
		return io.ErrShortWrite
	}
	w.currentSize += entrySize

	w.writeCount++
	if w.writeCount >= w.syncEvery {
		if err := w.file.Sync(); err != nil {
			return err
		}
		w.writeCount = 0
	}

	if w.currentSize > 52428800 { // 50MB
		if err := w.rotate(); err != nil {
			return err
		}
	}

	return nil
}

// rotate closes the current file, renames it to path+".bak", and opens a fresh file at the original path.
// Must be called with w.mu lock held.
func (w *WAL) rotate() error {
	if err := w.file.Close(); err != nil {
		return err
	}

	bakPath := w.path + ".bak"
	// Remove backup file if it exists to avoid rename issues on Windows
	_ = os.Remove(bakPath)

	if err := os.Rename(w.path, bakPath); err != nil {
		return err
	}

	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.file = file
	w.currentSize = 0
	w.writeCount = 0
	return nil
}

// Close flushes any unsynced writes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	var syncErr error
	if w.writeCount > 0 {
		syncErr = w.file.Sync()
	}

	closeErr := w.file.Close()
	w.file = nil

	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Recover plays back all valid entries from path+".bak" (if it exists) and path (the main log)
// into the sharded cache, returning the highest sequence number found.
func Recover(path string, sc *cache.ShardedCache) (uint64, error) {
	var highestSequence uint64

	// 1. Try to open path+".bak" first. If it exists, replay it fully.
	bakPath := path + ".bak"
	if _, err := os.Stat(bakPath); err == nil {
		seq, err := recoverFile(bakPath, sc)
		if err == nil {
			if seq > highestSequence {
				highestSequence = seq
			}
		} else {
			log.Printf("WAL Recovery warning: failed to recover backup file %s: %v", bakPath, err)
		}
	}

	// 2. Then open path. If it does not exist, return (highestSequence, nil) or (0, nil).
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return highestSequence, nil
	}

	seq, err := recoverFile(path, sc)
	if err != nil {
		return highestSequence, err
	}
	if seq > highestSequence {
		highestSequence = seq
	}

	return highestSequence, nil
}

// recoverFile reads a single WAL file and replays its valid entries into the cache.
func recoverFile(path string, sc *cache.ShardedCache) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var highestSequence uint64

	for {
		// a. Read exactly 4 bytes -> CRC32 (Big-Endian uint32)
		var crcBuf [4]byte
		_, err := io.ReadFull(reader, crcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return highestSequence, err
		}
		checksum := binary.BigEndian.Uint32(crcBuf[:])

		// b. Read exactly 8 bytes -> Sequence (Big-Endian uint64)
		var seqBuf [8]byte
		_, err = io.ReadFull(reader, seqBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return highestSequence, err
		}
		seq := binary.BigEndian.Uint64(seqBuf[:])

		// c. Read exactly 4 bytes -> Length (Big-Endian uint32)
		var lenBuf [4]byte
		_, err = io.ReadFull(reader, lenBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return highestSequence, err
		}
		length := binary.BigEndian.Uint32(lenBuf[:])

		// d. Read exactly Length bytes -> Payload
		payloadBytes := make([]byte, length)
		_, err = io.ReadFull(reader, payloadBytes)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Treat as truncated tail
				break
			}
			return highestSequence, err
		}

		// e. Recompute CRC32 over (Sequence bytes + Length bytes + Payload bytes)
		h := crc32.NewIEEE()
		h.Write(seqBuf[:])
		h.Write(lenBuf[:])
		h.Write(payloadBytes)
		expectedChecksum := h.Sum32()

		// f. If checksum mismatch: log.Printf a warning, skip this entry, continue the loop
		if checksum != expectedChecksum {
			log.Printf("WAL Recovery Warning: Checksum mismatch for entry with sequence number %d. Expected %d, got %d", seq, expectedChecksum, checksum)
			continue
		}

		// h. If valid: call sc.Set(payload, payload) to replay into cache
		payloadStr := string(payloadBytes)
		sc.Set(payloadStr, payloadStr)

		if seq > highestSequence {
			highestSequence = seq
		}
	}

	return highestSequence, nil
}
