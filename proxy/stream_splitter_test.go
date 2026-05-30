package proxy

import "testing"

func TestTagPrefixSuffixLen(t *testing.T) {
	cases := []struct {
		name string
		s    string
		tag  string
		want int
	}{
		{"empty string", "", "<thinking>", 0},
		{"no angle bracket", "hello world", "<thinking>", 0},
		{"full tag is not a partial suffix", "<thinking>", "<thinking>", 10},
		{"half open tag", "abc<thin", "<thinking>", 5},
		{"single lt", "abc<", "<thinking>", 1},
		{"half close tag", "secret</think", "</thinking>", 7},
		{"lt in middle only", "a<b c", "<thinking>", 0},
		{"trailing lt after text", "done<", "</thinking>", 1},
		{"longer than tag", "xx<thinking>yy", "<thinking>", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := tagPrefixSuffixLen(c.s, c.tag); got != c.want {
				t.Fatalf("tagPrefixSuffixLen(%q,%q)=%d, want %d", c.s, c.tag, got, c.want)
			}
		})
	}
}
