package topic

import "testing"

func TestValidateFilter(t *testing.T) {
	good := []string{"a/b/c", "a/+/c", "a/#", "#", "+/b", "a/b/"}
	for _, f := range good {
		if err := ValidateFilter(f); err != nil {
			t.Errorf("filter %q rejected: %v", f, err)
		}
	}
	bad := []string{"", "a/#/c", "a+", "a/b#", "a/#b", "a/+/c#"}
	for _, f := range bad {
		if err := ValidateFilter(f); err == nil {
			t.Errorf("filter %q unexpectedly accepted", f)
		}
	}
}

func TestValidatePublishTopic(t *testing.T) {
	if err := ValidatePublishTopic("a/b"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "a/+/c", "a/#"} {
		if err := ValidatePublishTopic(bad); err == nil {
			t.Errorf("topic %q unexpectedly accepted", bad)
		}
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/x", false},
		{"a/#", "a/b/c", true},
		{"a/#", "a", true},
		{"#", "x/y/z", true},
		{"+/b", "a/b", true},
		{"+/b", "a/c", false},
		{"a/b", "a/b/c", false},
		{"a/b/", "a/b/", true},
		{"a/b/", "a/b", false},
		{"a", "", false},
	}
	for _, tc := range cases {
		if got := Match(tc.filter, tc.topic); got != tc.want {
			t.Errorf("Match(%q,%q)=%v want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}
