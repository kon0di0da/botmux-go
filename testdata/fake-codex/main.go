package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const sessionID = "01234567-89ab-cdef-0123-456789abcdef"

func main() {
	home := os.Getenv("CODEX_HOME")
	if home == "" {
		panic("CODEX_HOME is required")
	}
	if argvPath := os.Getenv("FAKE_CODEX_ARGV_PATH"); argvPath != "" {
		_ = os.WriteFile(argvPath, []byte(strings.Join(os.Args[1:], "\n")), 0o600)
	}
	rolloutDir := filepath.Join(home, "sessions", "2026", "09", "08")
	if err := os.MkdirAll(rolloutDir, 0o700); err != nil {
		panic(err)
	}
	rolloutPath := filepath.Join(rolloutDir, "rollout-2026-09-08T00-00-00-"+sessionID+".jsonl")
	rollout, err := os.OpenFile(rolloutPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	defer rollout.Close()
	fmt.Print("› ")

	reader := bufio.NewReader(os.Stdin)
	var body strings.Builder
	collecting := false
	for {
		chunk, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		chunk = strings.TrimSuffix(chunk, "\n")
		if !collecting {
			if !strings.Contains(chunk, "\x1b[200~") {
				continue
			}
			collecting = true
			chunk = strings.TrimPrefix(chunk, "\x1b[200~")
		}
		if strings.Contains(chunk, "\x1b[201~") {
			chunk = strings.TrimSuffix(chunk, "\x1b[201~")
			body.WriteString(chunk)
			input := body.String()
			body.Reset()
			collecting = false
			appendTurn(home, rollout, input)
			fmt.Print("\n› ")
			continue
		}
		body.WriteString(chunk)
		body.WriteByte('\n')
	}
}

func appendTurn(home string, rollout *os.File, input string) {
	historyPath := filepath.Join(home, "history.jsonl")
	history, err := os.OpenFile(historyPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	defer history.Close()
	writeJSON(history, map[string]any{"session_id": sessionID, "text": input})
	writeJSON(rollout, map[string]any{
		"type": "response_item",
		"payload": map[string]any{
			"type": "message", "role": "user",
			"content": []map[string]string{{"type": "input_text", "text": input}},
		},
	})
	writeJSON(rollout, map[string]any{
		"type": "event_msg",
		"payload": map[string]any{
			"type": "task_complete", "turn_id": "fake-turn-1",
			"last_agent_message": "fake final: " + input,
		},
	})
}

func writeJSON(file *os.File, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		panic(err)
	}
}
