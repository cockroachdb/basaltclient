package blob

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/basaltclient/basaltpb"
)

// mockQuorumClient is a test double for quorumClient that allows precise control
// over when AppendSync calls complete.
type mockQuorumClient struct {
	mu              sync.Mutex
	appendSyncStart chan struct{} // closed when AppendSync is called
	appendSyncDone  chan error    // send error (or nil) to complete AppendSync
	startClosed     bool
}

func newMockQuorumClient() *mockQuorumClient {
	return &mockQuorumClient{
		appendSyncStart: make(chan struct{}),
		appendSyncDone:  make(chan error),
	}
}

func (m *mockQuorumClient) AppendSync(id ObjectID, offset uint64, data []byte) error {
	m.mu.Lock()
	if !m.startClosed {
		m.startClosed = true
		close(m.appendSyncStart)
	}
	m.mu.Unlock()
	return <-m.appendSyncDone
}

func (m *mockQuorumClient) Close() error {
	return nil
}

// waitForStart blocks until AppendSync has been called on this client.
func (m *mockQuorumClient) waitForStart(t *testing.T) {
	t.Helper()
	select {
	case <-m.appendSyncStart:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for AppendSync to start")
	}
}

// complete signals AppendSync to return with the given error.
func (m *mockQuorumClient) complete(err error) {
	m.appendSyncDone <- err
}

// reset prepares the client for another request.
func (m *mockQuorumClient) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendSyncStart = make(chan struct{})
	m.appendSyncDone = make(chan error)
	m.startClosed = false
}

// TestQuorumWriterLateResult tests that late results from a previous request
// are not counted towards subsequent requests.
//
// Scenario:
// 1. Request 1 is sent to 3 replicas
// 2. Workers 0 and 1 complete quickly, quorum is reached, request 1 returns
// 3. Worker 2 is still processing request 1 (slow)
// 4. Request 2 is sent to all 3 replicas
// 5. Worker 2 finally completes request 1 (late result!)
// 6. Only worker 0 completes request 2
//
// Expected: Request 2 should NOT have returned yet (needs 2 successes, only has 1)
// Bug: Late result from request 1 is counted towards request 2, causing early return
func TestQuorumWriterLateResult(t *testing.T) {
	// Create 3 mock clients
	clients := []*mockQuorumClient{
		newMockQuorumClient(),
		newMockQuorumClient(),
		newMockQuorumClient(),
	}

	// Track which client we're creating
	clientIdx := 0
	factory := func(addr string) quorumClient {
		c := clients[clientIdx]
		clientIdx++
		return c
	}

	objectID := ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	replicas := []*basaltpb.ReplicaInfo{
		{Addr: "addr0"},
		{Addr: "addr1"},
		{Addr: "addr2"},
	}

	w := newQuorumWriterWithFactory(objectID, replicas, factory)
	defer w.Close()

	// --- Request 1 ---
	var req1Done atomic.Bool
	var req1Err error
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		req1Err = w.WriteAndSync([]byte("request1"))
		req1Done.Store(true)
	}()

	// Wait for all 3 workers to start processing request 1
	clients[0].waitForStart(t)
	clients[1].waitForStart(t)
	clients[2].waitForStart(t)

	// Complete workers 0 and 1 (quorum = 2)
	clients[0].complete(nil)
	clients[1].complete(nil)

	// Wait for request 1 to complete
	wg1.Wait()
	if req1Err != nil {
		t.Fatalf("request 1 failed: %v", req1Err)
	}

	// Worker 2 is still processing request 1 (slow replica)
	// Reset clients 0 and 1 for request 2
	clients[0].reset()
	clients[1].reset()

	// --- Request 2 ---
	var req2Done atomic.Bool
	var req2Err error
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		req2Err = w.WriteAndSync([]byte("request2"))
		req2Done.Store(true)
	}()

	// Wait for workers 0 and 1 to start processing request 2
	clients[0].waitForStart(t)
	clients[1].waitForStart(t)

	// Now the slow worker 2 finally completes request 1 (LATE RESULT!)
	// This should NOT be counted towards request 2
	clients[2].complete(nil)

	// Give some time for the late result to be processed
	time.Sleep(50 * time.Millisecond)

	// Complete only worker 0 for request 2
	clients[0].complete(nil)

	// Give some time for result to be processed
	time.Sleep(50 * time.Millisecond)

	// At this point, request 2 should NOT have completed yet:
	// - It needs quorum of 2
	// - Only worker 0 has completed request 2
	// - The late result from worker 2 (request 1) should NOT count
	if req2Done.Load() {
		t.Fatal("BUG: request 2 completed early - late result from request 1 was counted!")
	}

	// Now complete worker 1 for request 2 (achieving real quorum)
	clients[1].complete(nil)

	// Wait for request 2 to complete
	done := make(chan struct{})
	go func() {
		wg2.Wait()
		close(done)
	}()

	select {
	case <-done:
		if req2Err != nil {
			t.Fatalf("request 2 failed: %v", req2Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for request 2 to complete")
	}

	// Note: Worker 2 may still be blocked waiting for request 2 completion.
	// The defer w.Close() will clean up all workers.
}
