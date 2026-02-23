package basaltclient

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/basaltclient/basaltpb"
)

// AliasResolver resolves alias names to controller addresses.
type AliasResolver interface {
	Resolve(name string) ([]string, error)
}

// ParsedPath represents a fully parsed Basalt path.
type ParsedPath struct {
	// Controllers is the list of controller addresses.
	// Empty for local paths.
	Controllers []string
	// Path is the namespace path (e.g., "/dir/file") without query string.
	Path string
	// Config is the parsed replication policy with defaults applied.
	Config basaltpb.ReplicationPolicy
}

// IsLocal returns true if this represents a local filesystem path.
func (p *ParsedPath) IsLocal() bool {
	return p == nil
}

// SplitPath splits the path into directory and base name components.
// For "/a/b/c", it returns ("/a/b", "c").
// For "/a", it returns ("", "a").
// For "/", it returns ("", "").
func (p *ParsedPath) SplitPath() (dir, base string) {
	if p.Path == "/" || p.Path == "" {
		return "", ""
	}
	path := strings.TrimPrefix(p.Path, "/")
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return "", path
	}
	return "/" + path[:idx], path[idx+1:]
}

// ParsePath parses a Basalt path with optional query parameters.
//
// Supported formats:
//   - "//host:port/path?query" - direct controller address
//   - "//host1:port,host2:port/path" - multiple controllers
//   - "//alias/path?query" - named alias (resolved via resolver)
//   - "basalt://..." - same formats with scheme prefix
//   - "/local/path" - returns nil (local filesystem path)
//
// When the address part contains no colon or comma, it is treated as an
// alias and resolved via the resolver.
//
// Query parameters for replication configuration:
//   - ssd=N: Number of SSD replicas (default 3)
//   - hdd=N: Number of HDD replicas (default 0)
//   - archive: Enable cloud object storage tier (presence means true)
//   - zone=cross|local: Placement strategy (default cross)
//
// When zone=local is specified, localZone must be provided.
// When an alias path is parsed, resolver must be provided.
//
// Returns nil for local paths (no // or basalt:// prefix).
func ParsePath(path string, localZone string, resolver AliasResolver) (*ParsedPath, error) {
	// Strip optional "basalt:" scheme prefix.
	path = strings.TrimPrefix(path, "basalt:")

	// Local path - no Basalt prefix.
	if !strings.HasPrefix(path, "//") {
		return nil, nil
	}

	rest := path[2:]

	var controllers []string
	var namespacePath string

	// Split off query string first.
	queryIdx := strings.Index(rest, "?")
	var queryStr string
	if queryIdx >= 0 {
		queryStr = rest[queryIdx+1:]
		rest = rest[:queryIdx]
	}

	// Find where the path starts (first / after the address part).
	idx := strings.Index(rest, "/")
	var addrPart string
	if idx < 0 {
		addrPart = rest
		namespacePath = "/"
	} else {
		addrPart = rest[:idx]
		namespacePath = rest[idx:]
	}

	if addrPart == "" {
		return nil, fmt.Errorf("empty controller address or alias")
	}

	// If the address part contains no colon or comma, treat it as an
	// alias and resolve via the resolver.
	if !strings.ContainsAny(addrPart, ":,") {
		if resolver == nil {
			return nil, fmt.Errorf("alias %q requires resolver", addrPart)
		}
		resolved, err := resolver.Resolve(addrPart)
		if err != nil {
			return nil, fmt.Errorf("resolving alias %q: %w", addrPart, err)
		}
		parsed := &ParsedPath{
			Controllers: resolved,
			Path:        namespacePath,
		}
		if err := parsed.Config.Parse(queryStr, localZone); err != nil {
			return nil, err
		}
		return parsed, nil
	}

	// Split comma-separated addresses.
	controllers = strings.Split(addrPart, ",")
	for i, addr := range controllers {
		controllers[i] = strings.TrimSpace(addr)
		if controllers[i] == "" {
			return nil, fmt.Errorf("empty controller address in list")
		}
	}

	parsed := &ParsedPath{
		Controllers: controllers,
		Path:        namespacePath,
	}
	if err := parsed.Config.Parse(queryStr, localZone); err != nil {
		return nil, err
	}
	return parsed, nil
}

// IsBasaltPath returns true if the path is a Basalt path.
// Accepts paths starting with "//" or "basalt://".
func IsBasaltPath(path string) bool {
	return strings.HasPrefix(path, "//") || strings.HasPrefix(path, "basalt://")
}
