package worker

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// The dashboard's TypeScript types are checked against dashboard/testdata/status.json.
// This test pins that file to the Go structs, so renaming a JSON field on either side fails here.
func TestStatusFixtureMatchesGoStruct(t *testing.T) {
	raw, err := os.ReadFile("../../dashboard/testdata/status.json")
	if err != nil {
		t.Fatal(err)
	}
	var st NodeStatus
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		t.Fatalf("fixture has a field NodeStatus does not: %v", err)
	}

	out, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var want, got map[string]any
	_ = json.Unmarshal(raw, &want)
	_ = json.Unmarshal(out, &got)
	// handoffs[].error is omitempty and the fixture sets it, so compare nested keys too.
	if !reflect.DeepEqual(keys(want), keys(got)) {
		t.Fatalf("NodeStatus no longer serializes the fixture's keys:\nfixture %v\ngo      %v", keys(want), keys(got))
	}
	if !reflect.DeepEqual(keys(want["handoffs"].([]any)[0]), keys(got["handoffs"].([]any)[0])) {
		t.Fatalf("HandoffRecord keys differ from the fixture")
	}
}

func keys(v any) []string {
	m, _ := v.(map[string]any)
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func TestNodeStatusReady(t *testing.T) {
	cases := []struct {
		st   NodeStatus
		want bool
	}{
		{NodeStatus{LeaderID: "w-0", Healthy: true}, true},
		{NodeStatus{LeaderID: "", Healthy: true}, false},
		{NodeStatus{LeaderID: "w-0", Healthy: false}, false},
	}
	for _, c := range cases {
		if got := c.st.Ready(); got != c.want {
			t.Errorf("%+v Ready() = %v, want %v", c.st, got, c.want)
		}
	}
}
