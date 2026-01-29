package blob

import (
	"bufio"
	"io"
	"net"

	"github.com/cockroachdb/errors"
)

// DataClient is a client for communicating with a blob server's data endpoint.
// It uses net.Buffers (writev) for efficient zero-copy writes and
// a buffered reader for responses.
//
// DataClient is NOT safe for concurrent use. Callers must ensure exclusive
// access, either by using a pool (which provides exclusive access via
// acquire/release semantics) or by using a dedicated client per goroutine.
type DataClient struct {
	addr   string
	conn   net.Conn
	r      *bufio.Reader
	hdrBuf [RequestHeaderSize]byte // reusable buffer for request headers
	// ioBufs is a pre-allocated backing array for net.Buffers to avoid
	// allocations when doing gather writes (writev). tmpBufs is a slice
	// header that points to ioBufs, avoiding escape of a local slice header.
	ioBufs  [2][]byte
	tmpBufs net.Buffers
}

// NewDataClient creates a new data client for the given server address.
// The connection is established lazily on the first operation.
func NewDataClient(addr string) *DataClient {
	return &DataClient{addr: addr}
}

// Close closes the connection to the server.
func (c *DataClient) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.r = nil
		return err
	}
	return nil
}

func (c *DataClient) ensureConnected() error {
	if c.conn != nil {
		return nil
	}
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return errors.Wrapf(err, "connecting to %s", c.addr)
	}
	c.conn = conn
	c.r = bufio.NewReader(conn)
	return nil
}

// doRequest sends a request and reads the response. src is the data to send
// with the request (may be nil). dst is the buffer to read response data into
// (may be nil if no response data is expected). Returns the number of bytes
// read into dst.
func (c *DataClient) doRequest(hdr RequestHeader, src, dst []byte) (int, error) {
	if err := c.ensureConnected(); err != nil {
		return 0, err
	}

	// Encode header into our reusable buffer.
	hdr.Encode(c.hdrBuf[:])

	// Send request using net.Buffers (writev) to avoid copying data.
	// This performs a gather write of header + data in a single syscall.
	// We use the pre-allocated ioBufs array and tmpBufs slice header to avoid
	// allocations. tmpBufs is a field so its address doesn't escape.
	c.ioBufs[0] = c.hdrBuf[:]
	if len(src) > 0 {
		c.ioBufs[1] = src
		c.tmpBufs = c.ioBufs[:2]
	} else {
		c.tmpBufs = c.ioBufs[:1]
	}
	_, err := c.tmpBufs.WriteTo(c.conn)
	c.ioBufs[1] = nil // clear reference to data to allow GC
	if err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return 0, errors.Wrap(err, "writing request")
	}

	// Read response header.
	respHdr, err := ReadResponseHeader(c.r)
	if err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return 0, err
	}

	// Read response data into dst if provided.
	var n int
	if respHdr.Length > 0 {
		if len(dst) > 0 {
			// Read directly into dst, up to its capacity.
			readLen := min(respHdr.Length, uint64(len(dst)))
			if _, err := io.ReadFull(c.r, dst[:readLen]); err != nil {
				_ = c.conn.Close()
				c.conn = nil
				return 0, errors.Wrap(err, "reading response data")
			}
			n = int(readLen)
			// Discard any extra bytes beyond dst capacity.
			if respHdr.Length > uint64(len(dst)) {
				extra := respHdr.Length - uint64(len(dst))
				if _, err := io.CopyN(io.Discard, c.r, int64(extra)); err != nil {
					_ = c.conn.Close()
					c.conn = nil
					return n, errors.Wrap(err, "discarding extra response data")
				}
			}
		} else {
			// No dst provided, discard response data.
			if _, err := io.CopyN(io.Discard, c.r, int64(respHdr.Length)); err != nil {
				_ = c.conn.Close()
				c.conn = nil
				return 0, errors.Wrap(err, "discarding response data")
			}
		}
	}

	if err := respHdr.Status.Error(); err != nil {
		return 0, err
	}

	return n, nil
}

// Append appends data to an object at the specified offset.
func (c *DataClient) Append(id ObjectID, offset uint64, data []byte) error {
	_, err := c.doRequest(RequestHeader{
		OpCode:   OpAppend,
		ObjectID: id,
		Offset:   offset,
		Length:   uint64(len(data)),
	}, data, nil)
	return err
}

// AppendSync appends data to an object and syncs to disk in one round-trip.
func (c *DataClient) AppendSync(id ObjectID, offset uint64, data []byte) error {
	_, err := c.doRequest(RequestHeader{
		OpCode:   OpAppendSync,
		ObjectID: id,
		Offset:   offset,
		Length:   uint64(len(data)),
	}, data, nil)
	return err
}

// Sync syncs an object's data to disk.
func (c *DataClient) Sync(id ObjectID) error {
	// Sync is implemented as AppendSync with empty data.
	return c.AppendSync(id, 0, nil)
}

// Read reads data from an object at the specified offset into the provided
// buffer. Returns the number of bytes read.
func (c *DataClient) Read(id ObjectID, offset uint64, p []byte) (int, error) {
	return c.doRequest(RequestHeader{
		OpCode:   OpRead,
		ObjectID: id,
		Offset:   offset,
		Length:   uint64(len(p)),
	}, nil, p)
}

// Addr returns the server address this client connects to.
func (c *DataClient) Addr() string {
	return c.addr
}
