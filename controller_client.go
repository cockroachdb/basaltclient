package basaltclient

import (
	"context"
	"io"

	"github.com/cockroachdb/basaltclient/basaltpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ControllerClientConfig configures a ControllerClient.
type ControllerClientConfig struct {
	// Logger is the logger for diagnostic messages. If nil, DefaultLogger is used.
	Logger Logger
}

// ControllerClient is the controller gRPC client.
type ControllerClient struct {
	addr   string
	logger Logger
	conn   *grpc.ClientConn
	client basaltpb.ControllerClient
}

// NewControllerClient creates a new controller client connected to addr.
func NewControllerClient(addr string, cfg ...ControllerClientConfig) (*ControllerClient, error) {
	var c ControllerClientConfig
	if len(cfg) > 0 {
		c = cfg[0]
	}
	if c.Logger == nil {
		c.Logger = DefaultLogger
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &ControllerClient{
		addr:   addr,
		logger: c.Logger,
		conn:   conn,
		client: basaltpb.NewControllerClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *ControllerClient) Close() error {
	return c.conn.Close()
}

// Mount registers a Pebble instance and acquires exclusive write access to its store directory.
func (c *ControllerClient) Mount(
	ctx context.Context, instanceID string, zone string, clusterID []byte, storeID []byte,
) (*basaltpb.MountResponse, error) {
	return c.client.Mount(ctx, &basaltpb.MountRequest{
		InstanceId: instanceID,
		Zone:       zone,
		ClusterId:  basaltpb.UUIDFromBytes(clusterID),
		StoreId:    basaltpb.UUIDFromBytes(storeID),
	})
}

// Unmount releases the write lock on a store directory.
func (c *ControllerClient) Unmount(ctx context.Context, mountID []byte) error {
	_, err := c.client.Unmount(ctx, &basaltpb.UnmountRequest{
		MountId: basaltpb.UUIDFromBytes(mountID),
	})
	return err
}

// Mkdir creates a subdirectory within a directory.
func (c *ControllerClient) Mkdir(
	ctx context.Context, parentID []byte, name string,
) (basaltpb.UUID, error) {
	resp, err := c.client.Mkdir(ctx, &basaltpb.MkdirRequest{
		ParentId: basaltpb.UUIDFromBytes(parentID),
		Name:     name,
	})
	if err != nil {
		return basaltpb.UUID{}, err
	}
	return resp.DirectoryId, nil
}

// Rmdir removes an empty directory.
func (c *ControllerClient) Rmdir(ctx context.Context, parentID []byte, name string) error {
	_, err := c.client.Rmdir(ctx, &basaltpb.RmdirRequest{
		ParentId: basaltpb.UUIDFromBytes(parentID),
		Name:     name,
	})
	return err
}

// Create allocates a new file in a directory and selects replicas.
func (c *ControllerClient) Create(
	ctx context.Context, directoryID []byte, name string, policy *basaltpb.ReplicationPolicy,
) (*basaltpb.ObjectMeta, error) {
	resp, err := c.client.Create(ctx, &basaltpb.CreateRequest{
		DirectoryId: basaltpb.UUIDFromBytes(directoryID),
		Name:        name,
		Policy:      policy,
	})
	if err != nil {
		return nil, err
	}
	return resp.Meta, nil
}

// StatByID returns metadata for an object by ID.
// If includeReferences is true, all namespace references (hardlinks) are included.
// If includeZombies is true, the object is returned even if it is a zombie
// (scheduled for deletion).
func (c *ControllerClient) StatByID(
	ctx context.Context, objectID []byte, includeReferences bool, includeZombies bool,
) (*basaltpb.StatResponse, error) {
	return c.client.StatByID(ctx, &basaltpb.StatByIDRequest{
		ObjectId:          basaltpb.UUIDFromBytes(objectID),
		IncludeReferences: includeReferences,
		IncludeZombies:    includeZombies,
	})
}

// StatByPath returns metadata for an object by (directory_id, name).
// If includeReferences is true, all namespace references (hardlinks) are included.
func (c *ControllerClient) StatByPath(
	ctx context.Context, directoryID []byte, name string, includeReferences bool,
) (*basaltpb.StatResponse, error) {
	return c.client.StatByPath(ctx, &basaltpb.StatByPathRequest{
		DirectoryId:       basaltpb.UUIDFromBytes(directoryID),
		Name:              name,
		IncludeReferences: includeReferences,
	})
}

// Unlink removes an entry from a directory.
func (c *ControllerClient) Unlink(
	ctx context.Context, directoryID []byte, name string,
) (*basaltpb.UnlinkResponse, error) {
	return c.client.Unlink(ctx, &basaltpb.UnlinkRequest{
		DirectoryId: basaltpb.UUIDFromBytes(directoryID),
		Name:        name,
	})
}

// Seal marks an object as immutable with its final size.
func (c *ControllerClient) Seal(ctx context.Context, objectID []byte, size int64) error {
	_, err := c.client.Seal(ctx, &basaltpb.SealRequest{
		ObjectId: basaltpb.UUIDFromBytes(objectID),
		Size_:    size,
	})
	return err
}

// Link creates a hardlink to an existing object in a directory.
func (c *ControllerClient) Link(
	ctx context.Context, directoryID []byte, name string, objectID []byte,
) error {
	_, err := c.client.Link(ctx, &basaltpb.LinkRequest{
		DirectoryId: basaltpb.UUIDFromBytes(directoryID),
		Name:        name,
		ObjectId:    basaltpb.UUIDFromBytes(objectID),
	})
	return err
}

// Rename moves an entry within the same directory.
func (c *ControllerClient) Rename(
	ctx context.Context, directoryID []byte, oldName string, newName string,
) error {
	_, err := c.client.Rename(ctx, &basaltpb.RenameRequest{
		DirectoryId: basaltpb.UUIDFromBytes(directoryID),
		OldName:     oldName,
		NewName:     newName,
	})
	return err
}

// List returns all entries in a directory.
func (c *ControllerClient) List(
	ctx context.Context, directoryID []byte,
) ([]basaltpb.DirectoryEntry, error) {
	stream, err := c.client.List(ctx, &basaltpb.ListRequest{
		DirectoryId: basaltpb.UUIDFromBytes(directoryID),
	})
	if err != nil {
		return nil, err
	}

	var entries []basaltpb.DirectoryEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}
