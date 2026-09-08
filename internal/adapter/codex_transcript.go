package adapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type codexTranscriptCursor struct {
	Path     string
	Offset   int64
	Armed    bool
	Input    string
	terminal bool
}

type codexRolloutRecord struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (c *codexTranscriptCursor) ReadNew() ([]AdapterEvent, error) {
	data, err := os.ReadFile(c.Path)
	if err != nil {
		return nil, err
	}
	if c.Offset < 0 || c.Offset > int64(len(data)) {
		c.Offset = 0
	}
	delta := data[c.Offset:]
	lastNewline := bytes.LastIndexByte(delta, '\n')
	if lastNewline < 0 {
		return nil, nil
	}
	complete := delta[:lastNewline]
	c.Offset += int64(lastNewline + 1)

	events := make([]AdapterEvent, 0, 2)
	for _, line := range bytes.Split(complete, []byte{'\n'}) {
		event, ok := c.parseLine(line)
		if ok {
			events = append(events, event...)
		}
	}
	return events, nil
}

func (c *codexTranscriptCursor) parseLine(line []byte) ([]AdapterEvent, bool) {
	var record codexRolloutRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return nil, false
	}
	var payload map[string]any
	if err := json.Unmarshal(record.Payload, &payload); err != nil {
		return nil, false
	}
	if record.Type == "response_item" && payload["type"] == "message" && payload["role"] == "user" {
		text := codexContentText(payload["content"], "input_text")
		if codexHistoryTextMatches(text, c.Input) {
			c.Armed = true
		}
		return nil, false
	}
	if !c.Armed || c.terminal || record.Type != "event_msg" {
		return nil, false
	}
	turnID, _ := payload["turn_id"].(string)
	if turnID == "" {
		return nil, false
	}
	switch payload["type"] {
	case "task_complete":
		final, _ := payload["last_agent_message"].(string)
		c.terminal = true
		return []AdapterEvent{
			{Kind: AdapterOutput, Output: final},
			{Kind: AdapterTurnTerminal, Status: TurnCompleted},
		}, true
	case "turn_aborted":
		reason, _ := payload["reason"].(string)
		c.terminal = true
		return []AdapterEvent{{
			Kind:        AdapterTurnTerminal,
			Status:      TurnAborted,
			ErrorCode:   "codex_turn_aborted",
			ErrorDetail: safeCodexErrorDetail(reason),
		}}, true
	}
	return nil, false
}

func codexContentText(raw any, contentType string) string {
	blocks, ok := raw.([]any)
	if !ok {
		return ""
	}
	var out strings.Builder
	for _, rawBlock := range blocks {
		block, ok := rawBlock.(map[string]any)
		if !ok || block["type"] != contentType {
			continue
		}
		if text, ok := block["text"].(string); ok {
			out.WriteString(text)
		}
	}
	return out.String()
}

func safeCodexErrorDetail(raw string) string {
	raw = strings.TrimSpace(raw)
	if len(raw) > 320 {
		raw = raw[:320]
	}
	return raw
}

func findCodexRolloutBySessionID(sessionID string) (string, bool) {
	if sessionID == "" {
		return "", false
	}
	root := filepath.Join(codexHome(), "sessions")
	suffix := "-" + sessionID + ".jsonl"
	var found string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if strings.HasSuffix(entry.Name(), suffix) {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	return found, err == nil && found != ""
}

func (c *codexTranscriptCursor) String() string {
	return fmt.Sprintf("codexTranscriptCursor(path=%s, offset=%d)", c.Path, c.Offset)
}
