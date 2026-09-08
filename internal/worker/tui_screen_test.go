package worker

import "testing"

func TestStripAnsiPreservesVisibleTextAndCursorSpacing(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "SGR colors",
			raw:  "\x1b[31mError:\x1b[0m failed",
			want: "Error: failed",
		},
		{
			name: "OSC hyperlink",
			raw:  "\x1b]8;;https://example.com\x1b\\link\x1b]8;;\x1b\\",
			want: "link",
		},
		{
			name: "cursor forward",
			raw:  "Yes\x1b[4CNo",
			want: "Yes    No",
		},
		{
			name: "DCS payload",
			raw:  "before\x1bP1;2|ignored\x1b\\after",
			want: "beforeafter",
		},
		{
			name: "incomplete CSI",
			raw:  "answer\x1b[38;5",
			want: "answer",
		},
		{
			name: "UTF-8 text",
			raw:  "中文回答",
			want: "中文回答",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripAnsi(tt.raw); got != tt.want {
				t.Fatalf("stripAnsi(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestLineDeduperPreservesRepeatedMeaningfulLines(t *testing.T) {
	deduper := NewLineDeduper(20)
	lines := []string{
		"}",
		"}",
		"OK",
		"OK",
		"node:fs:1363",
		"node:fs:1363",
		"The final answer is unchanged.",
		"The final answer is unchanged.",
	}

	for i, line := range lines {
		if got := deduper.Check(line); got != line {
			t.Fatalf("line %d (%q) was removed: got %q", i, line, got)
		}
	}
}

func TestLineDeduperDropsDynamicStatusRedraws(t *testing.T) {
	deduper := NewLineDeduper(20)

	first := "⠋ Thinking (1.0s · 120 tokens) 10%"
	second := "⠙ Thinking (1.2s · 140 tokens) 20%"
	if got := deduper.Check(first); got != first {
		t.Fatalf("first status = %q, want %q", got, first)
	}
	if got := deduper.Check(second); got != "" {
		t.Fatalf("redrawn status = %q, want empty", got)
	}

	different := "⠹ Running tests (1.4s · 160 tokens) 30%"
	if got := deduper.Check(different); got != different {
		t.Fatalf("different status = %q, want %q", got, different)
	}

	deduper.Reset()
	if got := deduper.Check(second); got != second {
		t.Fatalf("status after Reset = %q, want %q", got, second)
	}
}
