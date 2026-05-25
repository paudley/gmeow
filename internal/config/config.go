// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	currentConfigVersion = 1
	defaultConfigPath    = "gmeow.toml"
	unlockEnvName        = "GMEOW_SOPS_UNLOCK_KEY"
	configEnvName        = "GMEOW_CONFIG"
)

type Options struct {
	Path                          string
	allowPlaintextSecretsForTests bool
}

type Loaded struct {
	Path             string
	Config           Config
	Resolved         Resolved
	SecretReferences map[string]int
}

type Config struct {
	System     SystemConfig      `toml:"system"`
	Filestore  FilestoreConfig   `toml:"filestore"`
	Postgres   PostgresConfig    `toml:"postgres"`
	RabbitMQ   RabbitMQConfig    `toml:"rabbitmq"`
	Interfaces []InterfaceConfig `toml:"interfaces"`
	Sources    []SourceConfig    `toml:"sources"`
	Analysis   AnalysisConfig    `toml:"analysis"`
	Search     SearchConfig      `toml:"search"`
	Secrets    map[string]string `toml:"secrets"`
}

type SystemConfig struct {
	ConfigVersion int    `toml:"config_version"`
	InstanceID    string `toml:"instance_id"`
	DataDir       string `toml:"data_dir"`
}

type FilestoreConfig struct {
	Root string `toml:"root"`
}

type PostgresConfig struct {
	Enabled        bool   `toml:"enabled"`
	Host           string `toml:"host"`
	Port           int    `toml:"port"`
	Database       string `toml:"database"`
	User           string `toml:"user"`
	PasswordSecret string `toml:"password_secret"`
	SSLMode        string `toml:"ssl_mode"`
}

type RabbitMQConfig struct {
	Enabled   bool   `toml:"enabled"`
	URLSecret string `toml:"url_secret"`
}

type InterfaceConfig struct {
	Name    string `toml:"name"`
	Kind    string `toml:"kind"`
	Enabled bool   `toml:"enabled"`
	Host    string `toml:"host"`
	Port    int    `toml:"port"`
}

type SourceConfig struct {
	Name             string   `toml:"name"`
	Kind             string   `toml:"kind"`
	Enabled          bool     `toml:"enabled"`
	Facets           []string `toml:"facets"`
	Capabilities     []string `toml:"capabilities"`
	TokenSecret      string   `toml:"token_secret"`
	PrivateKeySecret string   `toml:"private_key_secret"`
}

type AnalysisConfig struct {
	Analyzers []AnalyzerConfig `toml:"analyzers"`
}

type AnalyzerConfig struct {
	Name       string   `toml:"name"`
	Version    string   `toml:"version"`
	Enabled    bool     `toml:"enabled"`
	WorkerKind string   `toml:"worker_kind"`
	MediaTypes []string `toml:"media_types"`
}

type SearchConfig struct {
	Backends []SearchBackendConfig `toml:"backends"`
}

type SearchBackendConfig struct {
	Name    string   `toml:"name"`
	Kind    string   `toml:"kind"`
	Enabled bool     `toml:"enabled"`
	Source  string   `toml:"source"`
	Facets  []string `toml:"facets"`
}

type Resolved struct {
	Postgres ResolvedPostgres
	RabbitMQ ResolvedRabbitMQ
	Sources  []ResolvedSource
	Worker   ResolvedWorker
}

type ResolvedPostgres struct {
	Enabled  bool
	Host     string
	Port     int
	Database string
	User     string
	Password string
	SSLMode  string
}

type ResolvedRabbitMQ struct {
	Enabled bool
	URL     string
}

type ResolvedSource struct {
	Name       string
	Kind       string
	Token      string
	PrivateKey string
}

type ResolvedWorker struct {
	Analyzers []AnalyzerConfig
}

func Load(options Options) (*Loaded, error) {
	path := selectedPath(options.Path)
	unlockKey, err := readUnlockKey()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	configBytes := raw
	sopsProtected := isSOPSConfig(path, raw)
	if sopsProtected {
		configBytes, err = decryptSOPS(path, unlockKey)
		if err != nil {
			return nil, err
		}
	}
	var generic map[string]any
	if _, err := toml.Decode(string(configBytes), &generic); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := rejectForbiddenPointers(generic); err != nil {
		return nil, err
	}
	var parsed Config
	metadata, err := toml.Decode(string(configBytes), &parsed)
	if err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return nil, fmt.Errorf("unknown config key %q", undecoded[0].String())
	}
	if parsed.Secrets == nil {
		parsed.Secrets = map[string]string{}
	}
	if err := validateConfig(parsed); err != nil {
		return nil, err
	}
	references := secretReferences(parsed)
	if len(references) > 0 && !sopsProtected && !options.allowPlaintextSecretsForTests {
		return nil, errors.New(
			"referenced secrets require a SOPS-protected config; plaintext secret leaves are not accepted",
		)
	}
	resolved, err := resolveSecrets(parsed, references)
	if err != nil {
		return nil, err
	}
	return &Loaded{
		Path:             path,
		Config:           parsed,
		Resolved:         resolved,
		SecretReferences: references,
	}, nil
}

func SecretNames(loaded *Loaded) []string {
	names := make([]string, 0, len(loaded.Config.Secrets))
	for name := range loaded.Config.Secrets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func selectedPath(path string) string {
	if strings.TrimSpace(path) != "" {
		return path
	}
	if envPath := strings.TrimSpace(os.Getenv(configEnvName)); envPath != "" {
		return envPath
	}
	return defaultConfigPath
}

func readUnlockKey() (string, error) {
	if value := strings.TrimSpace(os.Getenv(unlockEnvName)); value != "" {
		return value, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf(
			"%s is required and home directory cannot be resolved: %w",
			unlockEnvName,
			err,
		)
	}
	path := filepath.Join(home, ".config", "gmeow", "key.txt")
	value, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s or %s is required before startup", unlockEnvName, path)
	}
	key := strings.TrimSpace(string(value))
	if key == "" {
		return "", fmt.Errorf(
			"%s is empty; %s or %s is required before startup",
			path,
			unlockEnvName,
			path,
		)
	}
	return key, nil
}

func isSOPSConfig(path string, raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return strings.Contains(filepath.Base(path), ".sops.") ||
		bytes.Contains(trimmed, []byte("\n[sops]")) ||
		bytes.HasPrefix(trimmed, []byte("[sops]")) ||
		bytes.Contains(trimmed, []byte(`"sops"`))
}

func decryptSOPS(path, unlockKey string) ([]byte, error) {
	command := exec.Command(
		"sops",
		"decrypt",
		"--input-type",
		"binary",
		"--output-type",
		"binary",
		path,
	)
	command.Env = append(
		os.Environ(),
		unlockEnvName+"="+unlockKey,
		"SOPS_AGE_KEY="+unlockKey,
	)
	output, err := command.Output()
	if err == nil {
		return output, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return nil, fmt.Errorf(
			"decrypt config %s: %s",
			path,
			strings.TrimSpace(string(exitErr.Stderr)),
		)
	}
	return nil, fmt.Errorf("decrypt config %s: %w", path, err)
}

func rejectForbiddenPointers(value any) error {
	return rejectForbiddenPointersAt(value, nil)
}

func rejectForbiddenPointersAt(value any, path []string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			next := append(append([]string{}, path...), key)
			joined := strings.Join(next, ".")
			switch joined {
			case "secrets.file", "secrets.sops_file", "config.file":
				return fmt.Errorf("forbidden secondary config pointer %q", joined)
			}
			if err := rejectForbiddenPointersAt(child, next); err != nil {
				return err
			}
		}
	case []map[string]any:
		for _, child := range typed {
			if err := rejectForbiddenPointersAt(child, path); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := rejectForbiddenPointersAt(child, path); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateConfig(parsed Config) error {
	if parsed.System.ConfigVersion != currentConfigVersion {
		return fmt.Errorf("unsupported config version %d", parsed.System.ConfigVersion)
	}
	if strings.TrimSpace(parsed.System.InstanceID) == "" {
		return errors.New("system.instance_id is required")
	}
	if strings.TrimSpace(parsed.System.DataDir) == "" {
		return errors.New("system.data_dir is required")
	}
	if strings.TrimSpace(parsed.Filestore.Root) == "" {
		return errors.New("filestore.root is required")
	}
	if parsed.Postgres.Enabled {
		if strings.TrimSpace(parsed.Postgres.Host) == "" {
			return errors.New("postgres.host is required when postgres is enabled")
		}
		if parsed.Postgres.Port <= 0 {
			return errors.New("postgres.port must be positive when postgres is enabled")
		}
		if strings.TrimSpace(parsed.Postgres.Database) == "" {
			return errors.New("postgres.database is required when postgres is enabled")
		}
		if strings.TrimSpace(parsed.Postgres.User) == "" {
			return errors.New("postgres.user is required when postgres is enabled")
		}
		if strings.TrimSpace(parsed.Postgres.PasswordSecret) == "" {
			return errors.New("postgres.password_secret is required when postgres is enabled")
		}
	}
	if parsed.RabbitMQ.Enabled && strings.TrimSpace(parsed.RabbitMQ.URLSecret) == "" {
		return errors.New("rabbitmq.url_secret is required when rabbitmq is enabled")
	}
	for _, source := range parsed.Sources {
		if source.Enabled && strings.TrimSpace(source.Name) == "" {
			return errors.New("enabled source requires name")
		}
		if source.Enabled && strings.TrimSpace(source.Kind) == "" {
			return fmt.Errorf("source %q requires kind", source.Name)
		}
	}
	return nil
}

func secretReferences(parsed Config) map[string]int {
	references := map[string]int{}
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name != "" {
			references[name]++
		}
	}
	if parsed.Postgres.Enabled {
		add(parsed.Postgres.PasswordSecret)
	}
	if parsed.RabbitMQ.Enabled {
		add(parsed.RabbitMQ.URLSecret)
	}
	for _, source := range parsed.Sources {
		if source.Enabled {
			add(source.TokenSecret)
			add(source.PrivateKeySecret)
		}
	}
	return references
}

func resolveSecrets(parsed Config, references map[string]int) (Resolved, error) {
	for name := range references {
		if strings.TrimSpace(parsed.Secrets[name]) == "" {
			return Resolved{}, fmt.Errorf("referenced secret %q is missing or empty", name)
		}
	}
	resolved := Resolved{
		Postgres: ResolvedPostgres{
			Enabled:  parsed.Postgres.Enabled,
			Host:     parsed.Postgres.Host,
			Port:     parsed.Postgres.Port,
			Database: parsed.Postgres.Database,
			User:     parsed.Postgres.User,
			SSLMode:  parsed.Postgres.SSLMode,
		},
		RabbitMQ: ResolvedRabbitMQ{
			Enabled: parsed.RabbitMQ.Enabled,
		},
		Worker: ResolvedWorker{
			Analyzers: enabledAnalyzers(parsed.Analysis.Analyzers),
		},
	}
	if parsed.Postgres.Enabled {
		resolved.Postgres.Password = parsed.Secrets[parsed.Postgres.PasswordSecret]
	}
	if parsed.RabbitMQ.Enabled {
		resolved.RabbitMQ.URL = parsed.Secrets[parsed.RabbitMQ.URLSecret]
	}
	for _, source := range parsed.Sources {
		if !source.Enabled {
			continue
		}
		resolved.Sources = append(resolved.Sources, ResolvedSource{
			Name:       source.Name,
			Kind:       source.Kind,
			Token:      parsed.Secrets[source.TokenSecret],
			PrivateKey: parsed.Secrets[source.PrivateKeySecret],
		})
	}
	return resolved, nil
}

func enabledAnalyzers(analyzers []AnalyzerConfig) []AnalyzerConfig {
	enabled := make([]AnalyzerConfig, 0, len(analyzers))
	for _, analyzer := range analyzers {
		if analyzer.Enabled {
			enabled = append(enabled, analyzer)
		}
	}
	return enabled
}
