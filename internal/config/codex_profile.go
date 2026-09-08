package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const CodexProfileSuffix = ".config.toml"

var codexProfileNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

func CodexHome() string {
	if home := os.Getenv("CODEX_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".codex"
	}
	return filepath.Join(home, ".codex")
}

func DiscoverCodexProfiles() ([]string, error) {
	entries, err := os.ReadDir(CodexHome())
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("read Codex home: %w", err)
	}
	profiles := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), CodexProfileSuffix) {
			continue
		}
		profile := strings.TrimSuffix(entry.Name(), CodexProfileSuffix)
		if !codexProfileNamePattern.MatchString(profile) {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		profiles = append(profiles, profile)
	}
	sort.Strings(profiles)
	return profiles, nil
}

func ValidateCodexProfile(name string) (string, error) {
	if name == "" {
		return "", nil
	}
	if !codexProfileNamePattern.MatchString(name) {
		return "", fmt.Errorf("invalid profile name %q", name)
	}
	path := filepath.Join(CodexHome(), name+CodexProfileSuffix)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("profile %q not found", name)
		}
		return "", fmt.Errorf("stat profile %q: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("profile %q is not a regular file", name)
	}
	return name, nil
}
