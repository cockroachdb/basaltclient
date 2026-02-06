package blob

import (
	"sync"

	"github.com/cockroachdb/basaltclient/basaltpb"
)

// QuorumWriter implements dedicated WAL writing with quorum semantics.
// It uses persistent connections per replica (no pool contention) and
// persistent goroutines per replica (no spawn overhead). Writes complete
// when a quorum of replicas acknowledge, allowing lagging replicas to
// catch up asynchronously.
//
// QuorumWriter assumes serialized syncs - only one WriteAndSync call is in
// flight at a time. This matches Pebble's WAL sync behavior.
type QuorumWriter struct {
	objectID ObjectID
	quorum   int // number of replicas needed for quorum (2 for RF=3)

	mu            sync.Mutex
	cond          *sync.Cond
	offset        int64
	currentOffset int64 // offset of the current in-flight request (for filtering late results)
	successCount  int
	failureCount  int
	lastError     error
	workers       []*quorumReplicaWorker
	closed        bool
}

// quorumClient is the interface used by quorumReplicaWorker to communicate with replicas.
// This interface exists primarily for testing; production code uses *DataClient.
type quorumClient interface {
	AppendSync(id ObjectID, offset uint64, data []byte) error
	Close() error
}

// quorumReplicaWorker handles writes to a single replica.
type quorumReplicaWorker struct {
	w      *QuorumWriter // back-reference for reporting results
	addr   string
	client quorumClient // dedicated connection, not pooled

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []quorumRequest
	closed bool
}

// quorumRequest represents a single write+sync request.
type quorumRequest struct {
	offset uint64
	data   []byte
}

// quorumClientFactory creates a quorumClient for the given address.
// Used to inject mock clients for testing.
type quorumClientFactory func(addr string) quorumClient

// defaultQuorumClientFactory creates real DataClient instances.
func defaultQuorumClientFactory(addr string) quorumClient {
	return NewDataClient(addr)
}

// NewQuorumWriter creates a new quorum writer for the given object and replicas.
func NewQuorumWriter(objectID ObjectID, replicas []*basaltpb.ReplicaInfo) *QuorumWriter {
	return newQuorumWriterWithFactory(objectID, replicas, defaultQuorumClientFactory)
}

// newQuorumWriterWithFactory creates a new quorum writer using the provided client factory.
// This is primarily used for testing with mock clients.
func newQuorumWriterWithFactory(
	objectID ObjectID, replicas []*basaltpb.ReplicaInfo, factory quorumClientFactory,
) *QuorumWriter {
	quorum := (len(replicas) / 2) + 1 // majority quorum
	w := &QuorumWriter{
		objectID: objectID,
		quorum:   quorum,
		workers:  make([]*quorumReplicaWorker, len(replicas)),
	}
	w.cond = sync.NewCond(&w.mu)

	for i, r := range replicas {
		worker := &quorumReplicaWorker{
			w:      w,
			addr:   r.Addr,
			client: factory(r.Addr),
		}
		worker.cond = sync.NewCond(&worker.mu)
		w.workers[i] = worker
		go worker.run(objectID)
	}

	return w
}

// WriteAndSync writes data to all replicas and waits for quorum acknowledgment.
// It returns nil once a quorum of replicas have successfully written and synced
// the data. Lagging replicas will continue processing in the background.
//
// Only one WriteAndSync call may be in flight at a time.
func (w *QuorumWriter) WriteAndSync(data []byte) error {
	offset, workers, err := w.prepareWrite(data)
	if err != nil {
		return err
	}

	req := quorumRequest{
		offset: uint64(offset),
		data:   data,
	}

	// Fan out to all workers.
	for _, worker := range workers {
		worker.enqueue(req)
	}

	// Wait for quorum.
	return w.waitForQuorum(len(workers))
}

// prepareWrite prepares for a write operation, returning the offset and workers.
// Returns ErrClosed if the writer is closed.
func (w *QuorumWriter) prepareWrite(data []byte) (int64, []*quorumReplicaWorker, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, nil, ErrClosed
	}
	offset := w.offset
	w.offset += int64(len(data))
	w.currentOffset = offset // track current request for filtering late results
	w.successCount = 0
	w.failureCount = 0
	w.lastError = nil
	return offset, w.workers, nil
}

// waitForQuorum waits until quorum is reached or failure threshold is exceeded.
func (w *QuorumWriter) waitForQuorum(numWorkers int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.successCount < w.quorum && w.failureCount <= numWorkers-w.quorum {
		w.cond.Wait()
	}
	if w.successCount >= w.quorum {
		return nil
	}
	return w.lastError
}

// reportResult is called by workers when they complete a request.
// The offset parameter identifies which request this result belongs to.
// Results from previous requests (late arrivals) are ignored.
func (w *QuorumWriter) reportResult(offset uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Ignore late results from previous requests.
	if int64(offset) != w.currentOffset {
		return
	}
	if err == nil {
		w.successCount++
	} else {
		w.failureCount++
		w.lastError = err
	}
	w.cond.Signal()
}

// Close closes the quorum writer and all worker goroutines.
func (w *QuorumWriter) Close() error {
	workers, alreadyClosed := w.markClosed()
	if alreadyClosed {
		return nil
	}

	// Signal all workers to stop.
	for _, worker := range workers {
		worker.close()
	}

	// Wait for all workers to exit.
	// Workers close their own clients when they exit.
	for _, worker := range workers {
		worker.waitForExit()
	}

	return nil
}

// markClosed marks the writer as closed and returns the workers.
// Returns true if already closed.
func (w *QuorumWriter) markClosed() ([]*quorumReplicaWorker, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, true
	}
	w.closed = true
	return w.workers, false
}

// waitForExit blocks until the worker has exited.
func (rw *quorumReplicaWorker) waitForExit() {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	for !rw.closed {
		rw.cond.Wait()
	}
}

// enqueue adds a request to the worker's queue.
func (rw *quorumReplicaWorker) enqueue(req quorumRequest) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.queue = append(rw.queue, req)
	rw.cond.Signal()
}

// close signals the worker to stop processing.
func (rw *quorumReplicaWorker) close() {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.closed = true
	rw.cond.Signal()
}

// run is the worker goroutine that processes requests.
func (rw *quorumReplicaWorker) run(objectID ObjectID) {
	defer func() {
		_ = rw.client.Close()
		// Signal that we've exited.
		rw.mu.Lock()
		rw.closed = true
		rw.mu.Unlock()
		rw.cond.Broadcast()
	}()

	for {
		req, ok := rw.dequeue()
		if !ok {
			return // Closed
		}

		// Append + Sync in a single round-trip.
		err := rw.client.AppendSync(objectID, req.offset, req.data)

		// Report result to the writer, including offset to filter late results.
		rw.w.reportResult(req.offset, err)
	}
}

// dequeue waits for and returns the next request.
func (rw *quorumReplicaWorker) dequeue() (quorumRequest, bool) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	for len(rw.queue) == 0 && !rw.closed {
		rw.cond.Wait()
	}

	if len(rw.queue) == 0 {
		return quorumRequest{}, false
	}

	req := rw.queue[0]
	rw.queue = rw.queue[1:]
	return req, true
}

// ErrClosed is returned when operating on a closed quorum writer.
var ErrClosed = &closedError{}

type closedError struct{}

func (e *closedError) Error() string { return "quorum writer closed" }
