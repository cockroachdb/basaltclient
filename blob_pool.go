package basaltclient

import "sync"

const defaultPoolSize = 8

// BlobDataClientPool manages pooled connections to blob server data endpoints.
// It maintains separate per-server pools and provides exclusive access to
// clients via acquire/release semantics.
//
// BlobDataClientPool is safe for concurrent use from multiple goroutines.
type BlobDataClientPool struct {
	poolSize int
	mu       sync.Mutex
	pools    map[string]*serverPool
	closed   bool
}

// BlobDataClientPoolOption configures a BlobDataClientPool.
type BlobDataClientPoolOption func(*BlobDataClientPool)

// WithBlobPoolSize sets the maximum number of connections per server.
// The default is 8.
func WithBlobPoolSize(size int) BlobDataClientPoolOption {
	return func(p *BlobDataClientPool) {
		if size > 0 {
			p.poolSize = size
		}
	}
}

// serverPool manages a pool of BlobDataClient connections to a single server.
type serverPool struct {
	addr     string
	poolSize int
	mu       sync.Mutex
	cond     *sync.Cond
	clients  []*BlobDataClient // available clients (LIFO stack)
	count    int               // total created (available + in-use)
	closed   bool
}

// NewBlobDataClientPool creates a new data client pool.
func NewBlobDataClientPool(opts ...BlobDataClientPoolOption) *BlobDataClientPool {
	p := &BlobDataClientPool{
		poolSize: defaultPoolSize,
		pools:    make(map[string]*serverPool),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Acquire returns a BlobDataClient for the given server address. If all clients
// in the pool are in use, Acquire blocks until one becomes available.
// The returned client must be released via Release or ReleaseWithError.
func (p *BlobDataClientPool) Acquire(addr string) *BlobDataClient {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	sp := p.pools[addr]
	if sp == nil {
		sp = newServerPool(addr, p.poolSize)
		p.pools[addr] = sp
	}
	p.mu.Unlock()

	return sp.acquire()
}

// Release returns a healthy client to the pool for reuse. The client must
// have been obtained via Acquire and must not be used after calling Release.
func (p *BlobDataClientPool) Release(client *BlobDataClient) {
	if client == nil {
		return
	}
	p.mu.Lock()
	sp := p.pools[client.addr]
	p.mu.Unlock()

	if sp != nil {
		sp.release(client)
	}
}

// ReleaseWithError returns a client to the pool after an error occurred.
// The client's connection is closed and the slot is freed for a new connection.
// The client must have been obtained via Acquire and must not be used after
// calling ReleaseWithError.
func (p *BlobDataClientPool) ReleaseWithError(client *BlobDataClient) {
	if client == nil {
		return
	}
	p.mu.Lock()
	sp := p.pools[client.addr]
	p.mu.Unlock()

	if sp != nil {
		sp.releaseWithError(client)
	}
}

// Close closes all connections in all pools and prevents new acquisitions.
func (p *BlobDataClientPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	pools := p.pools
	p.pools = nil
	p.mu.Unlock()

	for _, sp := range pools {
		sp.close()
	}
	return nil
}

// newServerPool creates a new server pool for the given address.
func newServerPool(addr string, poolSize int) *serverPool {
	sp := &serverPool{
		addr:     addr,
		poolSize: poolSize,
		clients:  make([]*BlobDataClient, 0, poolSize),
	}
	sp.cond = sync.NewCond(&sp.mu)
	return sp
}

// acquire returns a BlobDataClient from the pool, blocking if necessary.
func (sp *serverPool) acquire() *BlobDataClient {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for {
		if sp.closed {
			return nil
		}

		// If there's an available client, return it (LIFO).
		if len(sp.clients) > 0 {
			client := sp.clients[len(sp.clients)-1]
			sp.clients = sp.clients[:len(sp.clients)-1]
			return client
		}

		// If we haven't reached the pool size limit, create a new client.
		if sp.count < sp.poolSize {
			sp.count++
			return NewBlobDataClient(sp.addr)
		}

		// Pool is at capacity, wait for a client to be released.
		sp.cond.Wait()
	}
}

// release returns a healthy client to the pool for reuse.
func (sp *serverPool) release(client *BlobDataClient) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.closed {
		_ = client.Close()
		return
	}

	// Return client to pool (LIFO).
	sp.clients = append(sp.clients, client)
	sp.cond.Signal()
}

// releaseWithError closes the client and frees the slot for a new connection.
func (sp *serverPool) releaseWithError(client *BlobDataClient) {
	_ = client.Close()

	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Decrement count to free the slot for a new connection.
	sp.count--
	sp.cond.Signal()
}

// close closes all clients in the pool and wakes up any waiting goroutines.
func (sp *serverPool) close() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.closed = true
	for _, client := range sp.clients {
		_ = client.Close()
	}
	sp.clients = nil
	sp.cond.Broadcast()
}
