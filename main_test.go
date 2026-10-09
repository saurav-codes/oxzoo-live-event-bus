package main

import (
	"strings"
	"testing"
)

func TestEventValidation(t *testing.T) {
	trace := "3f2b8c1e-9a4d-4e2b-8f1a-0c9d8e7f6a5b"
	cases := []struct {
		in     eventIn
		header string
		ok     bool
	}{
		{eventIn{Trace: trace, Kind: "zoo.chain", Text: "hello"}, "", true},
		{eventIn{Kind: "zoo.chain", Text: "hello"}, trace, true},
		{eventIn{Kind: "zoo.chain", Text: "hello"}, "", true},
		{eventIn{Kind: "zoo.chain", Text: "hello"}, "nope", false},
		{eventIn{Trace: "nope", Kind: "zoo.chain", Text: "hello"}, "", false},
		{eventIn{Kind: "Zoo Chain", Text: "hello"}, "", false},
		{eventIn{Kind: "zoo.chain", Text: ""}, "", false},
		{eventIn{Kind: "zoo.chain", Text: strings.Repeat("x", maxText+1)}, "", false},
	}
	for i, c := range cases {
		err := c.in.validate(c.header)
		if (err == nil) != c.ok {
			t.Errorf("case %d: err %v, want ok=%v", i, err, c.ok)
		}
	}
	in := eventIn{Kind: "k", Text: "t"}
	_ = in.validate(trace)
	if in.Trace != trace {
		t.Error("header trace not used")
	}
}
