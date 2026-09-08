package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type CliType string

const (
	CliMock   CliType = "mock"
	CliPty    CliType = "pty"
	CliAiden  CliType = "aiden"
	CliCodex  CliType = "codex"
	CliClaude CliType = "claude-code"
)

type BackendType string

const (
	BackendPty  BackendType = "pty"
	BackendTmux BackendType = "tmux"
)

type BotConfig struct {
	Name         string      `json:"name"`
	BotID        string      `json:"bot_id"`
	CliType      CliType     `json:"cli_type"`
	CliPath      string      `json:"cli_path,omitempty"`
	BackendType  BackendType `json:"backend_type,omitempty"`
	WorkingDir   string      `json:"working_dir,omitempty"`
	AllowedUsers []string    `json:"allowed_users,omitempty"`
	Model        string      `json:"model,omitempty"`
	CodexProfile string      `json:"codex_profile,omitempty"`
}

type DaemonConfig struct {
	ListenAddr    string      `json:"listen_addr"`
	DashboardAddr string      `json:"dashboard_addr"`
	LogLevel      string      `json:"log_level"`
	SessionsDir   string      `json:"sessions_dir"`
	Bots          []BotConfig `json:"bots"`
}

func DefaultConfig() *DaemonConfig {
	home, _ := os.UserHomeDir()
	sessionsDir := os.Getenv("BOTMUX_SESSIONS_DIR")
	if sessionsDir == "" {
		sessionsDir = filepath.Join(home, ".botmux-go", "sessions")
	}
	return &DaemonConfig{
		ListenAddr:    "127.0.0.1:17890",
		DashboardAddr: "127.0.0.1:17891",
		LogLevel:      "info",
		SessionsDir:   sessionsDir,
		Bots: []BotConfig{
			{
				Name:        "default",
				BotID:       "bot-default",
				CliType:     CliMock,
				BackendType: BackendPty,
				WorkingDir:  home,
			},
		},
	}
}

func Load(path string) (*DaemonConfig, error) {
	if path == "" {
		return LoadFromDefaultPath()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	return cfg, nil
}

func LoadFromDefaultPath() (*DaemonConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return DefaultConfig(), nil
	}
	paths := []string{
		filepath.Join(home, ".botmux-go", "bots.json"),
		"configs/bots.json",
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			absPath, _ := filepath.Abs(p)
			return Load(absPath)
		}
	}
	return DefaultConfig(), nil
}

func expandPath(p string) string {
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err == nil {
			p = filepath.Join(home, strings.TrimPrefix(p, "~"))
		}
	}
	return p
}

func (c *DaemonConfig) applyDefaults() {
	if c.ListenAddr == "" {
		c.ListenAddr = "127.0.0.1:17890"
	}
	if c.DashboardAddr == "" {
		c.DashboardAddr = "127.0.0.1:17891"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.SessionsDir == "" {
		envDir := os.Getenv("BOTMUX_SESSIONS_DIR")
		if envDir != "" {
			c.SessionsDir = expandPath(envDir)
		} else {
			home, _ := os.UserHomeDir()
			c.SessionsDir = filepath.Join(home, ".botmux-go", "sessions")
		}
	} else {
		c.SessionsDir = expandPath(c.SessionsDir)
	}
	for i := range c.Bots {
		if c.Bots[i].BackendType == "" {
			c.Bots[i].BackendType = BackendPty
		}
		if c.Bots[i].WorkingDir == "" {
			home, _ := os.UserHomeDir()
			c.Bots[i].WorkingDir = home
		} else {
			c.Bots[i].WorkingDir = expandPath(c.Bots[i].WorkingDir)
		}
	}
}

func (c *DaemonConfig) FindBot(botID string) (*BotConfig, bool) {
	for i := range c.Bots {
		if c.Bots[i].BotID == botID {
			return &c.Bots[i], true
		}
	}
	return nil, false
}

func (c *DaemonConfig) EnsureDirs() error {
	if c.SessionsDir != "" {
		if err := os.MkdirAll(c.SessionsDir, 0o755); err != nil {
			return err
		}
	}
	return nil
}
