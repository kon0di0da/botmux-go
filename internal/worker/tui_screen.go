package worker

import (
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	normSpinner = regexp.MustCompile(`[⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏⠄⠆⠖⠗⠚⠞⠛⣿⣻⣽⣾⣷⣯⣟⡿⢿⢸⣰⢀⡀]`)
	normStatus  = regexp.MustCompile(`[✻✳✶✴✦✧✢✣✤✥✽✼✺◐◓◑◒]`)
	normTimer   = regexp.MustCompile(`\(\d+(\.\d+)?s(\s*·[^)]*)?\)`)
	normPct     = regexp.MustCompile(`\d+(\.\d+)?%`)
	normSpaces  = regexp.MustCompile(`\s+`)
)

func stripAnsi(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	for i := 0; i < len(s); {
		switch s[i] {
		case 0x1b:
			if i+1 >= len(s) {
				return out.String()
			}
			switch s[i+1] {
			case '[':
				final := findCSIFinal(s, i+2)
				if final < 0 {
					return out.String()
				}
				if s[final] == 'C' {
					out.WriteString(strings.Repeat(" ", csiCount(s[i+2:final])))
				}
				i = final + 1
			case ']', 'P', 'X', '^', '_':
				i = skipControlString(s, i+2)
			case '(', ')', '*', '+':
				i += min(3, len(s)-i)
			default:
				i += 2
			}
		default:
			b := s[i]
			if (b < 0x20 && b != '\t' && b != '\n' && b != '\r') || b == 0x7f {
				i++
				continue
			}
			out.WriteByte(b)
			i++
		}
	}
	return out.String()
}

func findCSIFinal(s string, start int) int {
	for i := start; i < len(s); i++ {
		if s[i] >= 0x40 && s[i] <= 0x7e {
			return i
		}
	}
	return -1
}

func csiCount(params string) int {
	first := strings.SplitN(params, ";", 2)[0]
	first = strings.TrimLeft(first, "?><=")
	count, err := strconv.Atoi(first)
	if err != nil || count <= 0 {
		count = 1
	}
	return min(count, 1000)
}

func skipControlString(s string, start int) int {
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '\a':
			return i + 1
		case 0x1b:
			if i+1 < len(s) && s[i+1] == '\\' {
				return i + 2
			}
		}
	}
	return len(s)
}

func normalizeLine(s string) string {
	s = stripAnsi(s)
	s = normSpinner.ReplaceAllString(s, "")
	s = normStatus.ReplaceAllString(s, "")
	s = normTimer.ReplaceAllString(s, "")
	s = normPct.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	s = normSpaces.ReplaceAllString(s, " ")
	return s
}

type LineDeduper struct {
	seen    map[string]int
	order   []string
	maxSize int
}

func NewLineDeduper(maxSize int) *LineDeduper {
	if maxSize <= 0 {
		maxSize = 200
	}
	return &LineDeduper{
		seen:    make(map[string]int),
		order:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

func (d *LineDeduper) Check(raw string) string {
	visible := strings.TrimSpace(stripAnsi(raw))
	if visible == "" {
		return ""
	}
	if !isDynamicStatusLine(visible) {
		return raw
	}

	key := normalizeLine(visible)
	if key == "" {
		return ""
	}
	if _, ok := d.seen[key]; ok {
		return ""
	}
	if len(d.order) >= d.maxSize {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
	d.seen[key] = 1
	d.order = append(d.order, key)
	return raw
}

func isDynamicStatusLine(s string) bool {
	first, _ := utf8.DecodeRuneInString(s)
	if strings.ContainsRune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏⠄⠆⠖⠗⠚⠞⠛⣿⣻⣽⣾⣷⣯⣟⡿", first) {
		return true
	}
	return strings.ContainsRune("✻✳✶✴✦✧✢✣✤✥✽✼✺◐◓◑◒", first) &&
		(normTimer.MatchString(s) || normPct.MatchString(s))
}

func (d *LineDeduper) Reset() {
	d.seen = make(map[string]int)
	d.order = d.order[:0]
}
