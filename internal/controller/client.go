// Package controller provides the internal implementation of the controller client.
package controller

import (
	"context"
	"io"

	"github.com/cockroachdb/basaltclient/basaltpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the controller gRPC client.
type Client struct {
	addr   string
	conn   *grpc.ClientConn
	client basaltpb.ControllerClient
}

// New creates a new controller client connected to addr.
func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:   addr,
		conn:   conn,
		client: basaltpb.NewControllerClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Mount registers a Pebble instance and acquires exclusive write access to its store directory.
func (c *Client) Mount(
	ctx context.Context, instanceID string, az string, clusterID []byte, storeID []byte,
) (*basaltpb.MountResponse, error) {
	return c.client.Mount(ctx, &basaltpb.MountRequest{
		InstanceId: instanceID,
		Az:         az,
		ClusterId:  clusterID,
		StoreId:    storeID,
	})
}

// Unmount releases the write lock on a store directory.
func (c *Client) Unmount(ctx context.Context, mountID []byte) error {
	_, err := c.client.Unmount(ctx, &basaltpb.UnmountRequest{
		MountId: &basaltpb.MountID{Uuid: mountID},
	})
	return err
}

// Mkdir creates a subdirectory within a directory.
func (c *Client) Mkdir(
	ctx context.Context, parentID []byte, name string,
) (*basaltpb.DirectoryID, error) {
	resp, err := c.client.Mkdir(ctx, &basaltpb.MkdirRequest{
		ParentId: &basaltpb.DirectoryID{Uuid: parentID},
		Name:     name,
	})
	if err != nil {
		return nil, err
	}
	return resp.DirectoryId, nil
}

// Rmdir removes an empty directory.
func (c *Client) Rmdir(ctx context.Context, parentID []byte, name string) error {
	_, err := c.client.Rmdir(ctx, &basaltpb.RmdirRequest{
		ParentId: &basaltpb.DirectoryID{Uuid: parentID},
		Name:     name,
	})
	return err
}

// Create allocates a new file in a directory and selects replicas.
func (c *Client) Create(
	ctx context.Context, directoryID []byte, name string, config *basaltpb.ReplicationConfig,
) (*basaltpb.ObjectMeta, error) {
	resp, err := c.client.Create(ctx, &basaltpb.CreateRequest{
		DirectoryId: &basaltpb.DirectoryID{Uuid: directoryID},
		Name:        name,
		Config:      config,
	})
	if err != nil {
		return nil, err
	}
	return resp.Meta, nil
}

// LookupByID returns metadata for an object by ID.
func (c *Client) LookupByID(
	ctx context.Context, objectID []byte,
) (*basaltpb.LookupResponse, error) {
	return c.client.Lookup(ctx, &basaltpb.LookupRequest{
		Key: &basaltpb.LookupRequest_ObjectId{
			ObjectId: &basaltpb.ObjectID{Uuid: objectID},
		},
	})
}

// LookupByPath returns metadata for an object by (directory_id, name).
func (c *Client) LookupByPath(
	ctx context.Context, directoryID []byte, name string,
) (*basaltpb.LookupResponse, error) {
	return c.client.Lookup(ctx, &basaltpb.LookupRequest{
		Key: &basaltpb.LookupRequest_DirectoryLookup{
			DirectoryLookup: &basaltpb.DirectoryLookup{
				DirectoryId: &basaltpb.DirectoryID{Uuid: directoryID},
				Name:        name,
			},
		},
	})
}

// Delete removes an entry from a directory.
func (c *Client) Delete(
	ctx context.Context, directoryID []byte, name string,
) (*basaltpb.DeleteResponse, error) {
	return c.client.Delete(ctx, &basaltpb.DeleteRequest{
		DirectoryId: &basaltpb.DirectoryID{Uuid: directoryID},
		Name:        name,
	})
}

// Seal marks an object as immutable with its final size.
func (c *Client) Seal(ctx context.Context, objectID []byte, size int64) error {
	_, err := c.client.Seal(ctx, &basaltpb.SealRequest{
		ObjectId: &basaltpb.ObjectID{Uuid: objectID},
		Size:     size,
	})
	return err
}

// Link creates a hardlink to an existing object in a directory.
func (c *Client) Link(ctx context.Context, directoryID []byte, name string, objectID []byte) error {
	_, err := c.client.Link(ctx, &basaltpb.LinkRequest{
		DirectoryId: &basaltpb.DirectoryID{Uuid: directoryID},
		Name:        name,
		ObjectId:    &basaltpb.ObjectID{Uuid: objectID},
	})
	return err
}

// Rename moves an entry within the same directory.
func (c *Client) Rename(
	ctx context.Context, directoryID []byte, oldName string, newName string,
) error {
	_, err := c.client.Rename(ctx, &basaltpb.RenameRequest{
		DirectoryId: &basaltpb.DirectoryID{Uuid: directoryID},
		OldName:     oldName,
		NewName:     newName,
	})
	return err
}

// List returns all entries in a directory.
func (c *Client) List(ctx context.Context, directoryID []byte) ([]*basaltpb.DirectoryEntry, error) {
	stream, err := c.client.List(ctx, &basaltpb.ListRequest{
		DirectoryId: &basaltpb.DirectoryID{Uuid: directoryID},
	})
	if err != nil {
		return nil, err
	}

	var entries []*basaltpb.DirectoryEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
