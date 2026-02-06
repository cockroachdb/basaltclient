package basaltclient

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBlobDataClientPool_AcquireRelease(t *testing.T) {
	pool := NewBlobDataClientPool()
	defer pool.Close()

	addr := "localhost:26259"
	client := pool.Acquire(addr)
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.Addr() != addr {
		t.Fatalf("expected addr %s, got %s", addr, client.Addr())
	}

	pool.Release(client)

	// Acquire again should return the same client (LIFO).
	client2 := pool.Acquire(addr)
	if client2 != client {
		t.Fatal("expected same client from LIFO reuse")
	}
	pool.Release(client2)
}

func TestBlobDataClientPool_PoolSizeLimit(t *testing.T) {
	poolSize := 2
	pool := NewBlobDataClientPool(WithBlobPoolSize(poolSize))
	defer pool.Close()

	addr := "localhost:26259"

	// Acquire all clients.
	clients := make([]*BlobDataClient, poolSize)
	for i := range clients {
		clients[i] = pool.Acquire(addr)
		if clients[i] == nil {
			t.Fatalf("expected non-nil client at index %d", i)
		}
	}

	// Next acquire should block. Use a goroutine and channel to verify.
	acquired := make(chan *BlobDataClient, 1)
	go func() {
		acquired <- pool.Acquire(addr)
	}()

	// Give the goroutine time to block.
	select {
	case <-acquired:
		t.Fatal("acquire should have blocked")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked.
	}

	// Release one client.
	pool.Release(clients[0])

	// Now the blocked acquire should complete.
	select {
	case c := <-acquired:
		if c == nil {
			t.Fatal("expected non-nil client after release")
		}
		pool.Release(c)
	case <-time.After(time.Second):
		t.Fatal("acquire should have unblocked after release")
	}

	// Release remaining clients.
	for i := 1; i < len(clients); i++ {
		pool.Release(clients[i])
	}
}

func TestBlobDataClientPool_ReleaseWithError(t *testing.T) {
	poolSize := 2
	pool := NewBlobDataClientPool(WithBlobPoolSize(poolSize))
	defer pool.Close()

	addr := "localhost:26259"

	// Acquire all clients.
	client1 := pool.Acquire(addr)
	client2 := pool.Acquire(addr)

	// Release one with error (simulating connection failure).
	pool.ReleaseWithError(client1)

	// Acquire should create a new client since the slot was freed.
	client3 := pool.Acquire(addr)
	if client3 == nil {
		t.Fatal("expected non-nil client")
	}
	// client3 should be a new client, not client1.
	if client3 == client1 {
		t.Fatal("expected new client after ReleaseWithError")
	}

	pool.Release(client2)
	pool.Release(client3)
}

func TestBlobDataClientPool_MultipleServers(t *testing.T) {
	pool := NewBlobDataClientPool(WithBlobPoolSize(2))
	defer pool.Close()

	addr1 := "server1:26259"
	addr2 := "server2:26259"

	client1 := pool.Acquire(addr1)
	client2 := pool.Acquire(addr2)

	if client1.Addr() != addr1 {
		t.Fatalf("expected addr %s, got %s", addr1, client1.Addr())
	}
	if client2.Addr() != addr2 {
		t.Fatalf("expected addr %s, got %s", addr2, client2.Addr())
	}

	pool.Release(client1)
	pool.Release(client2)
}

func TestBlobDataClientPool_ConcurrentAccess(t *testing.T) {
	poolSize := 4
	pool := NewBlobDataClientPool(WithBlobPoolSize(poolSize))
	defer pool.Close()

	addr := "localhost:26259"
	numGoroutines := 20
	numIterations := 100

	var wg sync.WaitGroup
	var acquireCount atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				client := pool.Acquire(addr)
				if client == nil {
					t.Error("expected non-nil client")
					return
				}
				acquireCount.Add(1)
				// Simulate some work.
				time.Sleep(time.Microsecond)
				pool.Release(client)
			}
		}()
	}

	wg.Wait()

	expected := int64(numGoroutines * numIterations)
	if acquireCount.Load() != expected {
		t.Fatalf("expected %d acquires, got %d", expected, acquireCount.Load())
	}
}

func TestBlobDataClientPool_Close(t *testing.T) {
	pool := NewBlobDataClientPool(WithBlobPoolSize(2))

	addr := "localhost:26259"
	client := pool.Acquire(addr)
	pool.Release(client)

	// Close the pool.
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Acquire after close should return nil.
	client2 := pool.Acquire(addr)
	if client2 != nil {
		t.Fatal("expected nil client after pool close")
	}

	// Double close should be safe.
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected error on double close: %v", err)
	}
}

func TestBlobDataClientPool_CloseUnblocksWaiters(t *testing.T) {
	pool := NewBlobDataClientPool(WithBlobPoolSize(1))

	addr := "localhost:26259"
	client := pool.Acquire(addr)

	// Start a goroutine that will block on acquire.
	acquired := make(chan *BlobDataClient, 1)
	go func() {
		acquired <- pool.Acquire(addr)
	}()

	// Give the goroutine time to block.
	time.Sleep(50 * time.Millisecond)

	// Close the pool - should unblock the waiter.
	pool.Close()

	// Release the held client (after close).
	pool.Release(client)

	// The blocked acquire should have returned nil.
	select {
	case c := <-acquired:
		if c != nil {
			t.Fatal("expected nil client after pool close")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter should have been unblocked by close")
	}
}

func TestBlobDataClientPool_ReleaseNil(t *testing.T) {
	pool := NewBlobDataClientPool()
	defer pool.Close()

	// Should not panic.
	pool.Release(nil)
	pool.ReleaseWithError(nil)
}

func TestBlobDataClientPool_DefaultPoolSize(t *testing.T) {
	pool := NewBlobDataClientPool()
	defer pool.Close()

	if pool.poolSize != defaultPoolSize {
		t.Fatalf("expected default pool size %d, got %d", defaultPoolSize, pool.poolSize)
	}
}

func TestBlobDataClientPool_InvalidPoolSize(t *testing.T) {
	// Zero and negative sizes should use default.
	pool := NewBlobDataClientPool(WithBlobPoolSize(0))
	defer pool.Close()

	if pool.poolSize != defaultPoolSize {
		t.Fatalf("expected default pool size %d for size=0, got %d", defaultPoolSize, pool.poolSize)
	}

	pool2 := NewBlobDataClientPool(WithBlobPoolSize(-5))
	defer pool2.Close()

	if pool2.poolSize != defaultPoolSize {
		t.Fatalf("expected default pool size %d for size=-5, got %d", defaultPoolSize, pool2.poolSize)
	}
}
