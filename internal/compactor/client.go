// Package compactor provides the internal implementation of the compactor client.
package compactor

// Client is the internal compactor client implementation.
type Client struct {
	addr string
	// TODO: Add gRPC connection and client stub.
}

// New creates a new compactor client connected to addr.
func New(addr string) (*Client, error) {
	// TODO: Establish gRPC connection.
	return &Client{addr: addr}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	// TODO: Close gRPC connection.
	return nil
}
