package blob

import "sync"

const defaultPoolSize = 8

// DataClientPool manages pooled connections to blob server data endpoints.
// It maintains separate per-server pools and provides exclusive access to
// clients via acquire/release semantics.
//
// DataClientPool is safe for concurrent use from multiple goroutines.
type DataClientPool struct {
	poolSize int
	mu       sync.Mutex
	pools    map[string]*serverPool
	closed   bool
}

// DataClientPoolOption configures a DataClientPool.
type DataClientPoolOption func(*DataClientPool)

// WithPoolSize sets the maximum number of connections per server.
// The default is 8.
func WithPoolSize(size int) DataClientPoolOption {
	return func(p *DataClientPool) {
		if size > 0 {
			p.poolSize = size
		}
	}
}

// serverPool manages a pool of DataClient connections to a single server.
type serverPool struct {
	addr     string
	poolSize int
	mu       sync.Mutex
	cond     *sync.Cond
	clients  []*DataClient // available clients (LIFO stack)
	count    int           // total created (available + in-use)
	closed   bool
}

// NewDataClientPool creates a new data client pool.
func NewDataClientPool(opts ...DataClientPoolOption) *DataClientPool {
	p := &DataClientPool{
		poolSize: defaultPoolSize,
		pools:    make(map[string]*serverPool),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Acquire returns a DataClient for the given server address. If all clients
// in the pool are in use, Acquire blocks until one becomes available.
// The returned client must be released via Release or ReleaseWithError.
func (p *DataClientPool) Acquire(addr string) *DataClient {
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
func (p *DataClientPool) Release(client *DataClient) {
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
func (p *DataClientPool) ReleaseWithError(client *DataClient) {
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
func (p *DataClientPool) Close() error {
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
		clients:  make([]*DataClient, 0, poolSize),
	}
	sp.cond = sync.NewCond(&sp.mu)
	return sp
}

// acquire returns a DataClient from the pool, blocking if necessary.
func (sp *serverPool) acquire() *DataClient {
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
			return NewDataClient(sp.addr)
		}

		// Pool is at capacity, wait for a client to be released.
		sp.cond.Wait()
	}
}

// release returns a healthy client to the pool for reuse.
func (sp *serverPool) release(client *DataClient) {
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
func (sp *serverPool) releaseWithError(client *DataClient) {
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
