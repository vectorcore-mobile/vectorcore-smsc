package forwarder

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyedLocksSerialisesSameKeyOnly(t *testing.T) {
	var k keyedLocks
	var active, maxActive int32
	var wg sync.WaitGroup
	run := func(key string, counter *int32, max *int32) {
		defer wg.Done()
		release, err := k.acquire(context.Background(), key)
		if err != nil {
			t.Errorf("acquire(%s) error = %v", key, err)
			return
		}
		n := atomic.AddInt32(counter, 1)
		for {
			m := atomic.LoadInt32(max)
			if n <= m || atomic.CompareAndSwapInt32(max, m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(counter, -1)
		release()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go run("imsi-a", &active, &maxActive)
	}
	var otherActive, otherMax int32
	wg.Add(1)
	go run("imsi-b", &otherActive, &otherMax)
	wg.Wait()

	if maxActive != 1 {
		t.Fatalf("max concurrent holders of one key = %d, want 1", maxActive)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.locks) != 0 {
		t.Fatalf("locks map has %d entries after release, want 0", len(k.locks))
	}
}

func TestKeyedLocksAcquireHonoursContext(t *testing.T) {
	var k keyedLocks
	release, err := k.acquire(context.Background(), "imsi-a")
	if err != nil {
		t.Fatalf("acquire() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := k.acquire(ctx, "imsi-a"); err == nil {
		t.Fatal("second acquire succeeded while the key was held")
	}
	release()

	k.mu.Lock()
	n := len(k.locks)
	k.mu.Unlock()
	if n != 0 {
		t.Fatalf("locks map has %d entries after release and timeout, want 0", n)
	}
}
