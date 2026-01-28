// Package basaltclient provides clients for connecting to Basalt services.
//
// Basalt is a disaggregated storage layer for Pebble, consisting of:
//   - Controller: Coordinates object placement, mounts, and repairs
//   - Blob: Stores object data on local disks
//   - Compactor: Performs flush and compaction operations
//
// Usage:
//
//	ctrl := basaltclient.NewControllerClient(addr)
//	blobCtrl := basaltclient.NewBlobControlClient(grpcAddr)
//	blobData := basaltclient.NewBlobDataClient(dataAddr)
//	compactor := basaltclient.NewCompactorClient(addr)
package basaltclient

import (
	"github.com/cockroachdb/basaltclient/internal/blob"
	"github.com/cockroachdb/basaltclient/internal/compactor"
	"github.com/cockroachdb/basaltclient/internal/controller"
)

// ObjectID is a 16-byte unique identifier for an object.
type ObjectID = blob.ObjectID

// ControllerClient provides access to the Basalt controller service.
// The controller coordinates object placement, handles mount/seal operations,
// and manages replica repair.
type ControllerClient struct {
	client *controller.Client
}

// NewControllerClient creates a new client connected to the controller at addr.
func NewControllerClient(addr string) (*ControllerClient, error) {
	c, err := controller.New(addr)
	if err != nil {
		return nil, err
	}
	return &ControllerClient{client: c}, nil
}

// Close closes the controller client connection.
func (c *ControllerClient) Close() error {
	return c.client.Close()
}

// BlobControlClient provides gRPC access to blob server control operations
// (Create, Seal, Delete, Stat). For data operations (Append, Read), use
// BlobDataClient.
type BlobControlClient = blob.ControlClient

// NewBlobControlClient creates a new control client connected to the blob
// server's gRPC endpoint at addr (typically port 26258).
func NewBlobControlClient(addr string) (*BlobControlClient, error) {
	return blob.NewControlClient(addr)
}

// BlobDataClient provides TCP access to blob server data operations
// (Append, Read). For control operations (Create, Seal, Delete, Stat), use
// BlobControlClient.
//
// BlobDataClient is NOT safe for concurrent use. Callers must ensure exclusive
// access, either by using a pool or by using a dedicated client per goroutine.
type BlobDataClient = blob.DataClient

// NewBlobDataClient creates a new data client that will connect to the blob
// server's data endpoint at addr (typically port 26259).
// The connection is established lazily on first use.
func NewBlobDataClient(addr string) *BlobDataClient {
	return blob.NewDataClient(addr)
}

// QuorumWriter provides dedicated WAL writing with quorum semantics.
// It uses persistent connections per replica and persistent goroutines
// per replica. Writes complete when a quorum of replicas acknowledge,
// allowing lagging replicas to catch up asynchronously.
type QuorumWriter = blob.QuorumWriter

// NewQuorumWriter creates a new quorum writer for the given object and replicas.
func NewQuorumWriter(objectID ObjectID, replicas []string) *QuorumWriter {
	return blob.NewQuorumWriter(objectID, replicas)
}

// CompactorClient provides access to a Basalt compactor service.
// Compactors perform flush and compaction operations on behalf of mounts.
type CompactorClient struct {
	client *compactor.Client
}

// NewCompactorClient creates a new client connected to the compactor at addr.
func NewCompactorClient(addr string) (*CompactorClient, error) {
	c, err := compactor.New(addr)
	if err != nil {
		return nil, err
	}
	return &CompactorClient{client: c}, nil
}

// Close closes the compactor client connection.
func (c *CompactorClient) Close() error {
	return c.client.Close()
}
