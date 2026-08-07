package protocol

import (
	"bufio"
	"encoding/json"
	"io"
	"time"
)

type MessageType string

const (
	MsgNewSession  MessageType = "new_session"
	MsgUserInput   MessageType = "user_input"
	MsgClose       MessageType = "close"
	MsgOutput      MessageType = "output"
	MsgReady       MessageType = "ready"
	MsgError       MessageType = "error"
	MsgHeartbeat   MessageType = "heartbeat"
	MsgAck         MessageType = "ack"
)

type Message struct {
	Type      MessageType `json:"type"`
	SessionID string      `json:"session_id"`
	Payload   string      `json:"payload,omitempty"`
	Timestamp int64       `json:"ts"`
}

func NewMessage(typ MessageType, sessionID, payload string) *Message {
	return &Message{
		Type:      typ,
		SessionID: sessionID,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}
}

func (m *Message) Encode() ([]byte, error) {
	line, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	line = append(line, '\n')
	return line, nil
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	data, err := m.Encode()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(data)
	return int64(n), err
}

func DecodeMessage(r io.Reader) (*Message, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var msg Message
	if err := json.Unmarshal(line, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

type MessageReader struct {
	br *bufio.Reader
}

func NewMessageReader(r io.Reader) *MessageReader {
	return &MessageReader{br: bufio.NewReader(r)}
}

func (mr *MessageReader) Read() (*Message, error) {
	line, err := mr.br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var msg Message
	if err := json.Unmarshal(line, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
