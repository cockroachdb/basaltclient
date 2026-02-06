// Package basaltclient provides clients for connecting to Basalt services.
//
// Basalt is a disaggregated storage layer for Pebble, consisting of:
//   - Controller: Coordinates object placement, mounts, and repairs
//   - Blob: Stores object data on local disks
//
// Usage:
//
//	ctrl := basaltclient.NewControllerClient(addr)
//	blobCtrl := basaltclient.NewBlobControlClient(grpcAddr)
//	blobData := basaltclient.NewBlobDataClient(dataAddr)
package basaltclient

import (
	"context"

	"github.com/cockroachdb/basaltclient/basaltpb"
	"github.com/cockroachdb/basaltclient/internal/blob"
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

// Mount registers a Pebble instance and acquires exclusive write access to its store directory.
func (c *ControllerClient) Mount(
	ctx context.Context, instanceID string, zone string, clusterID []byte, storeID []byte,
) (*basaltpb.MountResponse, error) {
	return c.client.Mount(ctx, instanceID, zone, clusterID, storeID)
}

// Unmount releases the write lock on a store directory.
func (c *ControllerClient) Unmount(ctx context.Context, mountID []byte) error {
	return c.client.Unmount(ctx, mountID)
}

// Mkdir creates a subdirectory within a directory.
// Returns the UUID of the newly created directory.
func (c *ControllerClient) Mkdir(
	ctx context.Context, parentID []byte, name string,
) (basaltpb.UUID, error) {
	return c.client.Mkdir(ctx, parentID, name)
}

// Rmdir removes an empty directory.
func (c *ControllerClient) Rmdir(ctx context.Context, parentID []byte, name string) error {
	return c.client.Rmdir(ctx, parentID, name)
}

// Create allocates a new file in a directory and selects replicas.
func (c *ControllerClient) Create(
	ctx context.Context, directoryID []byte, name string, policy *basaltpb.ReplicationPolicy,
) (*basaltpb.ObjectMeta, error) {
	return c.client.Create(ctx, directoryID, name, policy)
}

// StatByID returns metadata for an object by ID.
func (c *ControllerClient) StatByID(
	ctx context.Context, objectID []byte,
) (*basaltpb.StatResponse, error) {
	return c.client.StatByID(ctx, objectID)
}

// StatByPath returns metadata for an object by (directory_id, name).
func (c *ControllerClient) StatByPath(
	ctx context.Context, directoryID []byte, name string,
) (*basaltpb.StatResponse, error) {
	return c.client.StatByPath(ctx, directoryID, name)
}

// Delete removes an entry from a directory.
func (c *ControllerClient) Delete(
	ctx context.Context, directoryID []byte, name string,
) (*basaltpb.DeleteResponse, error) {
	return c.client.Delete(ctx, directoryID, name)
}

// Seal marks an object as immutable with its final size.
func (c *ControllerClient) Seal(ctx context.Context, objectID []byte, size int64) error {
	return c.client.Seal(ctx, objectID, size)
}

// Link creates a hardlink to an existing object in a directory.
func (c *ControllerClient) Link(
	ctx context.Context, directoryID []byte, name string, objectID []byte,
) error {
	return c.client.Link(ctx, directoryID, name, objectID)
}

// Rename moves an entry within the same directory.
func (c *ControllerClient) Rename(
	ctx context.Context, directoryID []byte, oldName string, newName string,
) error {
	return c.client.Rename(ctx, directoryID, oldName, newName)
}

// List returns all entries in a directory.
func (c *ControllerClient) List(
	ctx context.Context, directoryID []byte,
) ([]*basaltpb.DirectoryEntry, error) {
	return c.client.List(ctx, directoryID)
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

// BlobDataClientPool manages pooled connections to blob server data endpoints.
// It maintains separate per-server pools and provides exclusive access to
// clients via acquire/release semantics.
type BlobDataClientPool = blob.DataClientPool

// BlobDataClientPoolOption configures a BlobDataClientPool.
type BlobDataClientPoolOption = blob.DataClientPoolOption

// WithBlobPoolSize sets the maximum number of connections per server.
// The default is 8.
func WithBlobPoolSize(size int) BlobDataClientPoolOption {
	return blob.WithPoolSize(size)
}

// NewBlobDataClientPool creates a new data client pool.
func NewBlobDataClientPool(opts ...BlobDataClientPoolOption) *BlobDataClientPool {
	return blob.NewDataClientPool(opts...)
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
