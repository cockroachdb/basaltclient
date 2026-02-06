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
