package basaltpb

import (
	"fmt"
	"net/url"
	"strconv"
)

// Parse parses a query string into the ReplicationConfig.
// It zeros out the receiver and applies defaults before parsing.
//
// Supported parameters:
//   - ssd=N: Number of SSD replicas (default 3)
//   - hdd=N: Number of HDD replicas (default 0)
//   - archive: Enable cloud object storage tier (presence means true)
//   - az=cross|local: Placement strategy (default cross)
//
// When az=local is specified, localAZ must be provided.
func (c *ReplicationConfig) Parse(queryStr, localAZ string) error {
	// Zero out and apply defaults.
	*c = ReplicationConfig{
		SsdReplicas: 3,
		HddReplicas: 0,
		Archive:     false,
		LocalAz:     "",
	}

	if queryStr == "" {
		return nil
	}

	values, err := url.ParseQuery(queryStr)
	if err != nil {
		return fmt.Errorf("invalid query string: %w", err)
	}

	// Known parameters.
	known := map[string]bool{
		"ssd":     true,
		"hdd":     true,
		"archive": true,
		"az":      true,
	}

	// Check for unknown parameters (to catch typos like "sssd=3").
	for key := range values {
		if !known[key] {
			return fmt.Errorf("unknown query parameter: %q", key)
		}
	}

	// Parse ssd parameter.
	if v := values.Get("ssd"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid ssd value %q: %w", v, err)
		}
		if n < 0 {
			return fmt.Errorf("ssd must be non-negative, got %d", n)
		}
		c.SsdReplicas = int32(n)
	}

	// Parse hdd parameter.
	if v := values.Get("hdd"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid hdd value %q: %w", v, err)
		}
		if n < 0 {
			return fmt.Errorf("hdd must be non-negative, got %d", n)
		}
		c.HddReplicas = int32(n)
	}

	// Parse archive parameter (presence means true).
	if _, ok := values["archive"]; ok {
		c.Archive = true
	}

	// Parse az parameter.
	if v := values.Get("az"); v != "" {
		switch v {
		case "cross":
			// Default, no-op.
		case "local":
			if localAZ == "" {
				return fmt.Errorf("az=local requires localAZ parameter")
			}
			c.LocalAz = localAZ
		default:
			return fmt.Errorf("invalid az value %q: must be \"cross\" or \"local\"", v)
		}
	}

	// Validate: ssd + hdd >= 1 unless archive is true.
	if c.SsdReplicas+c.HddReplicas < 1 && !c.Archive {
		return fmt.Errorf("at least one replica required (ssd + hdd >= 1) unless archive is enabled")
	}

	return nil
}
