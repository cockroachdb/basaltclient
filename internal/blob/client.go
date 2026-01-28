// Package blob provides the internal implementation of the blob client.
package blob

// Client is the internal blob client implementation.
type Client struct {
	addr string
	// TODO: Add gRPC connection and client stub.
}

// New creates a new blob client connected to addr.
func New(addr string) (*Client, error) {
	// TODO: Establish gRPC connection.
	return &Client{addr: addr}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	// TODO: Close gRPC connection.
	return nil
}
