package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

const (
	codexHistoryPollInterval = 100 * time.Millisecond
	codexHistoryWaitTimeout  = 3 * time.Second
)

var codexRolloutSessionPattern = regexp.MustCompile(
	`rollout-.*-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$`,
)

type codexHistoryEntry struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
}

func codexHome() string {
	if home := os.Getenv("CODEX_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".codex"
	}
	return filepath.Join(home, ".codex")
}

func codexHistoryPath() string {
	return filepath.Join(codexHome(), "history.jsonl")
}

func currentFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func matchCodexHistoryDelta(
	path string,
	from int64,
	expected string,
	accept func(string) bool,
) (string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	if int64(len(data)) <= from {
		return "", false
	}
	if from < 0 || from > int64(len(data)) {
		from = 0
	}
	delta := data[from:]
	lastNewline := bytes.LastIndexByte(delta, '\n')
	if lastNewline < 0 {
		return "", false
	}
	for _, line := range bytes.Split(delta[:lastNewline], []byte{'\n'}) {
		var entry codexHistoryEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if !codexHistoryTextMatches(entry.Text, expected) {
			continue
		}
		if accept != nil && !accept(entry.SessionID) {
			continue
		}
		if entry.SessionID == "" {
			continue
		}
		return entry.SessionID, true
	}
	return "", false
}

func waitForCodexHistory(
	ctx context.Context,
	path string,
	from int64,
	expected string,
	accept func(string) bool,
) (string, error) {
	deadline := time.NewTimer(codexHistoryWaitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(codexHistoryPollInterval)
	defer ticker.Stop()
	for {
		if sessionID, ok := matchCodexHistoryDelta(path, from, expected, accept); ok {
			return sessionID, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", fmt.Errorf("codex history confirmation timeout")
		case <-ticker.C:
		}
	}
}

func codexHistoryTextMatches(actual, expected string) bool {
	return normalizeCodexInput(actual) == normalizeCodexInput(expected)
}

func codexRolloutsOwnedByPID(pid int) (map[string]struct{}, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("invalid pid %d", pid)
	}
	targets, err := codexProcessOpenTargets(pid)
	if err != nil {
		return nil, err
	}
	owned := make(map[string]struct{})
	for _, target := range targets {
		if sessionID, ok := codexSessionIDFromRolloutPath(target); ok {
			owned[strings.ToLower(sessionID)] = struct{}{}
		}
	}
	return owned, nil
}

func codexProcessOpenTargets(pid int) ([]string, error) {
	if runtime.GOOS == "linux" {
		fdDir := fmt.Sprintf("/proc/%d/fd", pid)
		entries, err := os.ReadDir(fdDir)
		if err == nil {
			targets := make([]string, 0, len(entries))
			for _, entry := range entries {
				target, readErr := os.Readlink(filepath.Join(fdDir, entry.Name()))
				if readErr == nil {
					targets = append(targets, target)
				}
			}
			return targets, nil
		}
	}
	out, err := exec.Command("lsof", "-p", fmt.Sprintf("%d", pid), "-Fn").Output()
	if err != nil {
		return nil, fmt.Errorf("list open files for pid %d: %w", pid, err)
	}
	targets := make([]string, 0)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "n/") {
			targets = append(targets, strings.TrimPrefix(line, "n"))
		}
	}
	return targets, nil
}

func codexSessionIDFromRolloutPath(path string) (string, bool) {
	if !strings.Contains(filepath.ToSlash(path), "/sessions/") {
		return "", false
	}
	match := codexRolloutSessionPattern.FindStringSubmatch(filepath.ToSlash(path))
	if len(match) != 2 {
		return "", false
	}
	return match[1], true
}
