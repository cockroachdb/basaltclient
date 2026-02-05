package basaltclient

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

// testResolver is a simple AliasResolver for testing.
type testResolver struct {
	aliases map[string][]string
}

func (r *testResolver) Resolve(name string) ([]string, error) {
	if addrs, ok := r.aliases[name]; ok {
		return addrs, nil
	}
	return nil, fmt.Errorf("unknown alias: %s", name)
}

func TestParsePath(t *testing.T) {
	var localAZ string
	resolver := &testResolver{
		aliases: make(map[string][]string),
	}

	datadriven.RunTest(t, "testdata/path", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "local-az":
			// Set the local AZ for subsequent parse commands.
			localAZ = strings.TrimSpace(d.Input)
			return "ok"

		case "alias":
			// Register an alias: "name addr1,addr2,..."
			parts := strings.SplitN(strings.TrimSpace(d.Input), " ", 2)
			if len(parts) != 2 {
				return "error: alias requires 'name addrs'"
			}
			name := parts[0]
			addrs := strings.Split(parts[1], ",")
			for i := range addrs {
				addrs[i] = strings.TrimSpace(addrs[i])
			}
			resolver.aliases[name] = addrs
			return "ok"

		case "parse":
			// Parse a Basalt path.
			path := strings.TrimSpace(d.Input)

			parsed, err := ParsePath(path, localAZ, resolver)
			if err != nil {
				return fmt.Sprintf("error: %v", err)
			}
			if parsed == nil {
				return "nil (local path)"
			}

			var buf strings.Builder
			if len(parsed.Controllers) > 0 {
				fmt.Fprintf(&buf, "controllers: %s\n", strings.Join(parsed.Controllers, ", "))
			}
			fmt.Fprintf(&buf, "path: %s\n", parsed.Path)
			dir, base := parsed.SplitPath()
			fmt.Fprintf(&buf, "dir: %q\n", dir)
			fmt.Fprintf(&buf, "base: %q\n", base)
			fmt.Fprintf(&buf, "ssd: %d\n", parsed.Config.SsdReplicas)
			fmt.Fprintf(&buf, "hdd: %d\n", parsed.Config.HddReplicas)
			fmt.Fprintf(&buf, "archive: %v\n", parsed.Config.Archive)
			fmt.Fprintf(&buf, "local_az: %q", parsed.Config.LocalAz)
			return buf.String()

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}
