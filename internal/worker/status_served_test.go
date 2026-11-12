package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

const dashboardTypesFile = "../../dashboard/lib/cluster.ts"

type tsField struct {
	typ      string
	optional bool
}

// tsTypeFields returns the fields of `export type <name> = { ... };` in the
// dashboard's TS source. Declarations are split on ';', which the type bodies
// only use as a field separator.
func tsTypeFields(t *testing.T, src, name string) map[string]tsField {
	t.Helper()
	m := regexp.MustCompile(`(?s)export type ` + name + ` = \{(.*?)\};`).FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("type %s not found in %s", name, dashboardTypesFile)
	}
	field := regexp.MustCompile(`^(\w+)(\?)?:\s*(.+)$`)
	out := map[string]tsField{}
	for _, decl := range strings.Split(m[1], ";") {
		decl = strings.TrimSpace(decl)
		if decl == "" {
			continue
		}
		f := field.FindStringSubmatch(decl)
		if f == nil {
			t.Fatalf("cannot parse field %q in type %s", decl, name)
		}
		out[f[1]] = tsField{typ: strings.TrimSpace(f[3]), optional: f[2] == "?"}
	}
	return out
}

// checkAgainstTS fails if the served object and the TS type disagree on field
// names, on which fields may be absent, or on the JSON kind of any value. Kind
// matters as much as name: Go marshals a nil slice or map as null, which the
// dashboard's `.length` and `Object.entries` calls do not tolerate.
func checkAgainstTS(t *testing.T, what string, got map[string]any, want map[string]tsField) {
	t.Helper()
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("%s: worker serves %q but the TS type has no such field", what, k)
		}
	}
	for k, f := range want {
		v, ok := got[k]
		if !ok {
			if !f.optional {
				t.Errorf("%s: TS requires %q but the worker did not serve it", what, k)
			}
			continue
		}
		var kind string
		switch v.(type) {
		case string:
			kind = "string"
		case bool:
			kind = "boolean"
		case float64:
			kind = "number"
		case []any:
			kind = "array"
		case map[string]any:
			kind = "object"
		default:
			kind = "null"
		}
		var wantKind string
		switch {
		case f.typ == "string", f.typ == "boolean", f.typ == "number":
			wantKind = f.typ
		case strings.HasSuffix(f.typ, "[]"):
			wantKind = "array"
		case strings.HasPrefix(f.typ, "Record<"):
			wantKind = "object"
		default:
			t.Fatalf("%s: test does not know TS type %q of %q", what, f.typ, k)
		}
		if kind != wantKind {
			t.Errorf("%s: %q is JSON %s, TS declares %s (%s)", what, k, kind, wantKind, f.typ)
		}
	}
}

// TestStatusJSONMatchesDashboardType serves /status from a real single-node
// Raft leader and compares the JSON to the NodeStatus and Handoff types the
// dashboard compiles against.
func TestStatusJSONMatchesDashboardType(t *testing.T) {
	srcBytes, err := os.ReadFile(dashboardTypesFile)
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)

	n := startNode(t, "w-0", freePorts(t, 1)[0], t.TempDir(), true)
	t.Cleanup(n.stop)
	eventually(t, 10*time.Second, "w-0 becomes leader", n.raft.IsLeader)

	// Handoffs is only filled on a leader that has a Placer with something recorded.
	n.server.Placer = NewPlacer(n.raft, n.ring, func() ([]string, error) { return nil, nil })
	n.server.Placer.record(HandoffRecord{Node: "w-1", Epoch: 2, Targets: 1, SentAt: time.Now(), Acked: false, Error: "boom"})

	rec := httptest.NewRecorder()
	n.server.handleStatus(rec, httptest.NewRequest(http.MethodGet, statusPath, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, rec.Body)
	}

	checkAgainstTS(t, "NodeStatus", got, tsTypeFields(t, src, "NodeStatus"))

	handoffs, _ := got["handoffs"].([]any)
	if len(handoffs) != 1 {
		t.Fatalf("leader with a recorded handoff served handoffs = %v", got["handoffs"])
	}
	checkAgainstTS(t, "Handoff", handoffs[0].(map[string]any), tsTypeFields(t, src, "Handoff"))
}
