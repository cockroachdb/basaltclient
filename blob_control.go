package basaltclient

import (
	"context"

	"github.com/cockroachdb/basaltclient/basaltpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BlobControlClient provides gRPC access to blob server control operations
// (Create, Seal, Delete, Stat). Data operations (Append, Read) use BlobDataClient.
type BlobControlClient struct {
	addr   string
	conn   *grpc.ClientConn
	client basaltpb.BlobClient
}

// NewBlobControlClient creates a new control client connected to the blob server at addr.
func NewBlobControlClient(addr string) (*BlobControlClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &BlobControlClient{
		addr:   addr,
		conn:   conn,
		client: basaltpb.NewBlobClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *BlobControlClient) Close() error {
	return c.conn.Close()
}

// Create initializes a new object on this blob server.
func (c *BlobControlClient) Create(ctx context.Context, id ObjectID) error {
	_, err := c.client.Create(ctx, &basaltpb.BlobCreateRequest{
		Id: basaltpb.UUID(id),
	})
	return err
}

// Seal marks an object as immutable on this replica.
// Returns the final size of the sealed object.
func (c *BlobControlClient) Seal(ctx context.Context, id ObjectID) (int64, error) {
	resp, err := c.client.Seal(ctx, &basaltpb.BlobSealRequest{
		Id: basaltpb.UUID(id),
	})
	if err != nil {
		return 0, err
	}
	return resp.FinalSize, nil
}

// Delete removes an object from this blob server.
func (c *BlobControlClient) Delete(ctx context.Context, id ObjectID) error {
	_, err := c.client.Delete(ctx, &basaltpb.BlobDeleteRequest{
		Id: basaltpb.UUID(id),
	})
	return err
}

// Stat returns metadata about an object.
// Returns (size, sealed, error).
func (c *BlobControlClient) Stat(ctx context.Context, id ObjectID) (int64, bool, error) {
	resp, err := c.client.Stat(ctx, &basaltpb.BlobStatRequest{
		Id: basaltpb.UUID(id),
	})
	if err != nil {
		return 0, false, err
	}
	return resp.Size_, resp.Sealed, nil
}
