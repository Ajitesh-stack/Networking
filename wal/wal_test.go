package wal_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Ajitesh-stack/spatial-ingestion-server/cache"
	"github.com/Ajitesh-stack/spatial-ingestion-server/wal"
)

func makeWAL(t *testing.T) (*wal.WAL, string) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := wal.New(path, 1)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	return w, path
}

func TestBasicAppendAndRecover(t *testing.T) {
	w, path := makeWAL(t)

	payloads := []string{
		"client=abc,seq=1,lat=12.9,lon=77.6,weather=clear",
		"client=def,seq=2,lat=12.9,lon=77.6,weather=rainy",
		"client=ghi,seq=3,lat=12.9,lon=77.6,weather=sunny",
		"client=jkl,seq=4,lat=12.9,lon=77.6,weather=foggy",
		"client=mno,seq=5,lat=12.9,lon=77.6,weather=snowy",
	}

	for _, p := range payloads {
		if err := w.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sc := cache.NewShardedCache(16, 100)
	seq, err := wal.Recover(path, sc)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if seq != 5 {
		t.Errorf("Expected sequence 5, got %d", seq)
	}

	for _, p := range payloads {
		val, found := sc.Get(p)
		if !found {
			t.Errorf("Expected payload %q to be in cache", p)
		} else if val.(string) != p {
			t.Errorf("Expected cached value to be %q, got %q", p, val)
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	w, path := makeWAL(t)

	payloads := []string{
		"packet-1",
		"packet-2",
		"packet-3",
	}

	for _, p := range payloads {
		if err := w.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open the file in append mode and write 6 garbage bytes
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open file to append garbage: %v", err)
	}
	_, err = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02})
	if err != nil {
		f.Close()
		t.Fatalf("Failed to write garbage: %v", err)
	}
	f.Close()

	sc := cache.NewShardedCache(16, 100)
	seq, err := wal.Recover(path, sc)
	if err != nil {
		t.Fatalf("Recover failed with error: %v", err)
	}

	if seq != 3 {
		t.Errorf("Expected sequence 3, got %d", seq)
	}

	for _, p := range payloads {
		val, found := sc.Get(p)
		if !found {
			t.Errorf("Expected payload %q to be in cache", p)
		} else if val.(string) != p {
			t.Errorf("Expected cached value to be %q, got %q", p, val)
		}
	}
}

func TestCRCMismatchDetection(t *testing.T) {
	w, path := makeWAL(t)

	p1 := "good-packet-1"
	p2 := "corrupt-me"
	p3 := "good-packet-3"

	if err := w.Write(p1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Write(p2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Write(p3); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open wal.log, seek to the payload of the second entry
	// First entry = 16 header + 13 payload = 29 bytes.
	// Second entry starts at 29. Header is 16 bytes.
	// So second entry's payload starts at 29 + 16 = 45.
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file for random-access write: %v", err)
	}
	defer f.Close()

	offset := int64(16 + len(p1) + 16)
	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	_, err = f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	if err != nil {
		t.Fatalf("Write corrupt bytes failed: %v", err)
	}
	f.Close()

	sc := cache.NewShardedCache(16, 100)
	seq, err := wal.Recover(path, sc)
	if err != nil {
		t.Fatalf("Recover failed with error: %v", err)
	}

	// The third entry should still be replayed, so highest sequence should be 3
	if seq != 3 {
		t.Errorf("Expected sequence 3, got %d", seq)
	}

	// "good-packet-1" must be in cache
	if _, found := sc.Get(p1); !found {
		t.Errorf("Expected %q to be in cache", p1)
	}

	// "corrupt-me" must NOT be in cache
	if _, found := sc.Get(p2); found {
		t.Errorf("Expected %q NOT to be in cache", p2)
	}

	// "good-packet-3" must be in cache
	if _, found := sc.Get(p3); !found {
		t.Errorf("Expected %q to be in cache", p3)
	}
}

// Run with: go test -race ./wal/...
func TestConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := wal.New(path, 10)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 8
	numWrites := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numWrites; j++ {
				payload := fmt.Sprintf("worker-%d-write-%d", workerID, j)
				if err := w.Write(payload); err != nil {
					t.Errorf("Write failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sc := cache.NewShardedCache(16, 1000)
	seq, err := wal.Recover(path, sc)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if seq != 800 {
		t.Errorf("Expected highest sequence to be 800, got %d", seq)
	}

	// Verify all 800 unique entries are present in the cache
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numWrites; j++ {
			payload := fmt.Sprintf("worker-%d-write-%d", i, j)
			_, found := sc.Get(payload)
			if !found {
				t.Errorf("Expected payload %q to be in cache", payload)
			}
		}
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
