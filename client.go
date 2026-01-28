// Package basaltclient provides gRPC clients for connecting to Basalt services.
//
// Basalt is a disaggregated storage layer for Pebble, consisting of:
//   - Controller: Coordinates object placement, mounts, and repairs
//   - Blob: Stores object data on local disks
//   - Compactor: Performs flush and compaction operations
//
// Usage:
//
//	ctrl := basaltclient.NewControllerClient(addr)
//	blob := basaltclient.NewBlobClient(addr)
//	compactor := basaltclient.NewCompactorClient(addr)
package basaltclient

import (
	"github.com/cockroachdb/basaltclient/internal/blob"
	"github.com/cockroachdb/basaltclient/internal/compactor"
	"github.com/cockroachdb/basaltclient/internal/controller"
)

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

// BlobClient provides access to a Basalt blob server.
// Blob servers store object data on local disks and handle append, sync,
// seal, and read operations.
type BlobClient struct {
	client *blob.Client
}

// NewBlobClient creates a new client connected to the blob server at addr.
func NewBlobClient(addr string) (*BlobClient, error) {
	c, err := blob.New(addr)
	if err != nil {
		return nil, err
	}
	return &BlobClient{client: c}, nil
}

// Close closes the blob client connection.
func (c *BlobClient) Close() error {
	return c.client.Close()
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
