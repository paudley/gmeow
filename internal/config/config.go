// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	currentConfigVersion = 1
	defaultConfigPath    = "gmeow.toml"
	unlockEnvName        = "GMEOW_SOPS_UNLOCK_KEY"
	configEnvName        = "GMEOW_CONFIG"
	postgresPasswordName = "postgres_password"
	rabbitPasswordName   = "rabbitmq_password"
	testRabbitPassName   = "rabbitmq_test_password"
)

type Options struct {
	Path string
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
	RPC        RPCConfig         `toml:"rpc"`
	Postgres   PostgresConfig    `toml:"postgres"`
	RabbitMQ   RabbitMQConfig    `toml:"rabbitmq"`
	Scheduler  SchedulerConfig   `toml:"scheduler"`
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

type RPCConfig struct {
	Filestore RPCEndpointConfig `toml:"filestore"`
	Scheduler RPCEndpointConfig `toml:"scheduler"`
	Query     RPCEndpointConfig `toml:"query"`
}

type RPCEndpointConfig struct {
	Network string `toml:"network"`
	Address string `toml:"address"`
}

type PostgresConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Database string `toml:"database"`
	User     string `toml:"user"`
	SSLMode  string `toml:"ssl_mode"`
}

type RabbitMQConfig struct {
	Host      string `toml:"host"`
	Port      int    `toml:"port"`
	User      string `toml:"user"`
	VHost     string `toml:"vhost"`
	TestUser  string `toml:"test_user"`
	TestVHost string `toml:"test_vhost"`
}

type SchedulerConfig struct {
	ScanInterval         string            `toml:"scan_interval"`
	RetryLimit           int               `toml:"retry_limit"`
	RetryBackoff         string            `toml:"retry_backoff"`
	QueuePrefix          string            `toml:"queue_prefix"`
	Priorities           SchedulerPriority `toml:"priorities"`
	DeadLetterInspectMax int               `toml:"dead_letter_inspect_max"`
}

type SchedulerPriority struct {
	Interactive int `toml:"interactive"`
	Forced      int `toml:"forced"`
	FreshIngest int `toml:"fresh_ingest"`
	Repair      int `toml:"repair"`
	Background  int `toml:"background"`
}

type InterfaceConfig struct {
	Name string `toml:"name"`
	Kind string `toml:"kind"`
	Host string `toml:"host"`
	Port int    `toml:"port"`
}

type SourceConfig struct {
	Name         string   `toml:"name"`
	Kind         string   `toml:"kind"`
	Facets       []string `toml:"facets"`
	Capabilities []string `toml:"capabilities"`
}

type AnalysisConfig struct {
	Analyzers []AnalyzerConfig `toml:"analyzers"`
}

type AnalyzerConfig struct {
	Name       string   `toml:"name"`
	Version    string   `toml:"version"`
	WorkerKind string   `toml:"worker_kind"`
	MediaTypes []string `toml:"media_types"`
}

type SearchConfig struct {
	Backends []SearchBackendConfig `toml:"backends"`
}

type SearchBackendConfig struct {
	Name   string   `toml:"name"`
	Kind   string   `toml:"kind"`
	Source string   `toml:"source"`
	Facets []string `toml:"facets"`
}

type Resolved struct {
	Postgres  ResolvedPostgres
	RabbitMQ  ResolvedRabbitMQ
	RPC       ResolvedRPC
	Scheduler ResolvedScheduler
	Sources   []ResolvedSource
	Worker    ResolvedWorker
}

type ResolvedPostgres struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	SSLMode  string
}

type ResolvedRabbitMQ struct {
	URL     string
	TestURL string
}

type ResolvedRPC struct {
	Filestore ResolvedRPCEndpoint
	Scheduler ResolvedRPCEndpoint
	Query     ResolvedRPCEndpoint
}

type ResolvedRPCEndpoint struct {
	Network string
	Address string
}

type ResolvedScheduler struct {
	ScanInterval         string
	RetryLimit           int
	RetryBackoff         string
	QueuePrefix          string
	Priorities           SchedulerPriority
	DeadLetterInspectMax int
}

type ResolvedSource struct {
	Name string
	Kind string
}

type ResolvedWorker struct {
	Analyzers []AnalyzerConfig
}

func Load(options Options) (*Loaded, error) {
	path := selectedPath(options.Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	unlockKey, unlockKeyPath, err := readUnlockKey()
	if err != nil {
		return nil, err
	}
	var generic map[string]any
	if _, err := toml.Decode(string(raw), &generic); err != nil {
		if isOpaqueSOPSConfig(raw) {
			return nil, fmt.Errorf(
				"parse config %s: whole-file SOPS encryption is not supported for gmeow.toml; encrypt only [secrets] leaf values",
				path,
			)
		}
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := rejectForbiddenPointers(generic); err != nil {
		return nil, err
	}
	var parsed Config
	metadata, err := toml.Decode(string(raw), &parsed)
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
	resolvedSecretValues, err := resolveSecretLeaves(
		parsed.Secrets,
		references,
		unlockKey,
		unlockKeyPath,
	)
	if err != nil {
		return nil, err
	}
	parsed.Secrets = resolvedSecretValues
	resolved, err := resolveSecrets(parsed, references)
	if err != nil {
		return nil, err
	}
	if err := verifyDependencies(resolved); err != nil {
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

func readUnlockKey() (string, string, error) {
	if value := strings.TrimSpace(os.Getenv(unlockEnvName)); value != "" {
		return value, "", nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", fmt.Errorf(
			"%s is required and home directory cannot be resolved: %w",
			unlockEnvName,
			err,
		)
	}
	path := filepath.Join(home, ".config", "gmeow", "key.txt")
	value, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("%s or %s is required before startup", unlockEnvName, path)
	}
	key := strings.TrimSpace(string(value))
	if key == "" {
		return "", "", fmt.Errorf(
			"%s is empty; %s or %s is required before startup",
			path,
			unlockEnvName,
			path,
		)
	}
	return key, path, nil
}

func isOpaqueSOPSConfig(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return bytes.HasPrefix(trimmed, []byte(`{"data"`)) ||
		bytes.HasPrefix(trimmed, []byte("{\n\t\"data\"")) ||
		bytes.HasPrefix(trimmed, []byte("{\n  \"data\""))
}

func resolveSecretLeaves(
	secrets map[string]string,
	references map[string]int,
	unlockKey string,
	unlockKeyPath string,
) (map[string]string, error) {
	resolved := make(map[string]string, len(secrets))
	for name, value := range secrets {
		resolved[name] = value
	}
	for name := range references {
		value := strings.TrimSpace(secrets[name])
		if value == "" {
			return nil, fmt.Errorf("referenced secret %q is missing or empty", name)
		}
	}
	for name := range references {
		value := strings.TrimSpace(secrets[name])
		if !isSOPSLeaf(value) {
			return nil, fmt.Errorf(
				"referenced secret %q must be a SOPS-encrypted leaf value",
				name,
			)
		}
		decrypted, err := decryptSOPSLeaf(name, value, unlockKey, unlockKeyPath)
		if err != nil {
			return nil, err
		}
		resolved[name] = decrypted
	}
	return resolved, nil
}

func isSOPSLeaf(value string) bool {
	trimmed := strings.TrimSpace(value)
	return strings.HasPrefix(trimmed, "{") &&
		strings.Contains(trimmed, `"data"`) &&
		strings.Contains(trimmed, `"sops"`)
}

func decryptSOPSLeaf(name, value, unlockKey, unlockKeyPath string) (string, error) {
	command := exec.Command(
		"sops",
		"decrypt",
		"--input-type",
		"json",
		"--output-type",
		"binary",
		"/dev/stdin",
	)
	command.Stdin = strings.NewReader(value)
	env := append(os.Environ(), unlockEnvName+"="+unlockKey)
	if unlockKeyPath != "" {
		env = append(env, "SOPS_AGE_KEY_FILE="+unlockKeyPath)
	} else {
		env = append(env, "SOPS_AGE_KEY="+unlockKey)
	}
	command.Env = env
	output, err := command.Output()
	if err == nil {
		return string(output), nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return "", fmt.Errorf(
			"decrypt secret %q: %s",
			name,
			strings.TrimSpace(string(exitErr.Stderr)),
		)
	}
	return "", fmt.Errorf("decrypt secret %q: %w", name, err)
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
	if err := validateRPCEndpoint("rpc.filestore", parsed.RPC.Filestore); err != nil {
		return err
	}
	if err := validateRPCEndpoint("rpc.scheduler", parsed.RPC.Scheduler); err != nil {
		return err
	}
	if err := validateRPCEndpoint("rpc.query", parsed.RPC.Query); err != nil {
		return err
	}
	if strings.TrimSpace(parsed.Postgres.Host) == "" {
		return errors.New("postgres.host is required")
	}
	if parsed.Postgres.Port <= 0 {
		return errors.New("postgres.port must be positive")
	}
	if strings.TrimSpace(parsed.Postgres.Database) == "" {
		return errors.New("postgres.database is required")
	}
	if strings.TrimSpace(parsed.Postgres.User) == "" {
		return errors.New("postgres.user is required")
	}
	if strings.TrimSpace(parsed.RabbitMQ.Host) == "" {
		return errors.New("rabbitmq.host is required")
	}
	if parsed.RabbitMQ.Host != "127.0.0.1" {
		return errors.New("rabbitmq.host must be 127.0.0.1")
	}
	if parsed.RabbitMQ.Port <= 0 {
		return errors.New("rabbitmq.port must be positive")
	}
	if strings.TrimSpace(parsed.RabbitMQ.User) == "" {
		return errors.New("rabbitmq.user is required")
	}
	if strings.TrimSpace(parsed.RabbitMQ.VHost) == "" {
		return errors.New("rabbitmq.vhost is required")
	}
	if parsed.RabbitMQ.VHost != "gmeow" {
		return errors.New("rabbitmq.vhost must be gmeow")
	}
	if strings.TrimSpace(parsed.RabbitMQ.TestUser) == "" {
		return errors.New("rabbitmq.test_user is required")
	}
	if strings.TrimSpace(parsed.RabbitMQ.TestVHost) == "" {
		return errors.New("rabbitmq.test_vhost is required")
	}
	if parsed.RabbitMQ.TestVHost != "gmeow-test" {
		return errors.New("rabbitmq.test_vhost must be gmeow-test")
	}
	if strings.TrimSpace(parsed.Scheduler.QueuePrefix) != "" &&
		parsed.Scheduler.QueuePrefix != "gmeow." &&
		parsed.Scheduler.QueuePrefix != "gmeow.test." {
		return errors.New("scheduler.queue_prefix must be gmeow. or gmeow.test.")
	}
	for _, source := range parsed.Sources {
		if strings.TrimSpace(source.Name) == "" {
			return errors.New("source requires name")
		}
		if strings.TrimSpace(source.Kind) == "" {
			return fmt.Errorf("source %q requires kind", source.Name)
		}
	}
	return nil
}

func validateRPCEndpoint(name string, endpoint RPCEndpointConfig) error {
	network := strings.TrimSpace(endpoint.Network)
	address := strings.TrimSpace(endpoint.Address)
	if network == "" && address == "" {
		return nil
	}
	if network == "" {
		return fmt.Errorf("%s.network is required when address is set", name)
	}
	if address == "" {
		return fmt.Errorf("%s.address is required when network is set", name)
	}
	switch network {
	case "unix":
		if !filepath.IsAbs(address) {
			return fmt.Errorf("%s.address must be absolute for unix sockets", name)
		}
	case "tcp":
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("%s.address must be host:port for tcp: %w", name, err)
		}
		parsed := net.ParseIP(host)
		if parsed == nil || !parsed.IsLoopback() {
			return fmt.Errorf("%s.address must bind to a loopback IP", name)
		}
	default:
		return fmt.Errorf("%s.network must be unix or tcp", name)
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
	add(postgresPasswordName)
	add(rabbitPasswordName)
	add(testRabbitPassName)
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
			Host:     parsed.Postgres.Host,
			Port:     parsed.Postgres.Port,
			Database: parsed.Postgres.Database,
			User:     parsed.Postgres.User,
			SSLMode:  parsed.Postgres.SSLMode,
		},
		RPC:       resolvedRPC(parsed.RPC),
		Scheduler: resolvedScheduler(parsed.Scheduler),
		Worker: ResolvedWorker{
			Analyzers: configuredAnalyzers(parsed.Analysis.Analyzers),
		},
	}
	resolved.Postgres.Password = parsed.Secrets[postgresPasswordName]
	resolved.RabbitMQ.URL = rabbitMQURL(
		parsed.RabbitMQ.Host,
		parsed.RabbitMQ.Port,
		parsed.RabbitMQ.User,
		parsed.Secrets[rabbitPasswordName],
		parsed.RabbitMQ.VHost,
	)
	resolved.RabbitMQ.TestURL = rabbitMQURL(
		parsed.RabbitMQ.Host,
		parsed.RabbitMQ.Port,
		parsed.RabbitMQ.TestUser,
		parsed.Secrets[testRabbitPassName],
		parsed.RabbitMQ.TestVHost,
	)
	for _, source := range parsed.Sources {
		resolved.Sources = append(resolved.Sources, ResolvedSource{
			Name: source.Name,
			Kind: source.Kind,
		})
	}
	return resolved, nil
}

func resolvedRPC(raw RPCConfig) ResolvedRPC {
	return ResolvedRPC{
		Filestore: resolvedRPCEndpoint(
			raw.Filestore,
			"unix",
			"/run/gmeow/filestore.sock",
		),
		Scheduler: resolvedRPCEndpoint(
			raw.Scheduler,
			"unix",
			"/run/gmeow/scheduler.sock",
		),
		Query: resolvedRPCEndpoint(
			raw.Query,
			"unix",
			"/run/gmeow/query.sock",
		),
	}
}

func resolvedRPCEndpoint(
	raw RPCEndpointConfig,
	defaultNetwork string,
	defaultAddress string,
) ResolvedRPCEndpoint {
	network := strings.TrimSpace(raw.Network)
	if network == "" {
		network = defaultNetwork
	}
	address := strings.TrimSpace(raw.Address)
	if address == "" {
		address = defaultAddress
	}
	return ResolvedRPCEndpoint{Network: network, Address: address}
}

func rabbitMQURL(host string, port int, user, password, vhost string) string {
	return (&url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   vhost,
	}).String()
}

func verifyDependencies(resolved Resolved) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := verifyPostgres(ctx, resolved.Postgres); err != nil {
		return err
	}
	if err := verifyRabbitMQ("production", resolved.RabbitMQ.URL); err != nil {
		return err
	}
	if err := verifyRabbitMQ("test", resolved.RabbitMQ.TestURL); err != nil {
		return err
	}
	return nil
}

func verifyPostgres(ctx context.Context, postgres ResolvedPostgres) error {
	conn, err := pgx.Connect(ctx, postgresURL(postgres))
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer conn.Close(context.Background())
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}

func postgresURL(postgres ResolvedPostgres) string {
	values := url.Values{}
	if postgres.SSLMode != "" {
		values.Set("sslmode", postgres.SSLMode)
	}
	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(postgres.User, postgres.Password),
		Host:     fmt.Sprintf("%s:%d", postgres.Host, postgres.Port),
		Path:     postgres.Database,
		RawQuery: values.Encode(),
	}).String()
}

func verifyRabbitMQ(label, rawURL string) error {
	conn, err := amqp.DialConfig(rawURL, amqp.Config{
		Properties: amqp.Table{"connection_name": "gmeow.config-validate"},
	})
	if err != nil {
		return fmt.Errorf("connect rabbitmq %s: %w", label, err)
	}
	return conn.Close()
}

func configuredAnalyzers(analyzers []AnalyzerConfig) []AnalyzerConfig {
	return append([]AnalyzerConfig(nil), analyzers...)
}

func resolvedScheduler(raw SchedulerConfig) ResolvedScheduler {
	resolved := ResolvedScheduler{
		ScanInterval:         firstNonEmpty(raw.ScanInterval, "30s"),
		RetryLimit:           raw.RetryLimit,
		RetryBackoff:         firstNonEmpty(raw.RetryBackoff, "30s"),
		QueuePrefix:          firstNonEmpty(raw.QueuePrefix, "gmeow."),
		Priorities:           raw.Priorities,
		DeadLetterInspectMax: raw.DeadLetterInspectMax,
	}
	if resolved.RetryLimit <= 0 {
		resolved.RetryLimit = 3
	}
	if resolved.DeadLetterInspectMax <= 0 {
		resolved.DeadLetterInspectMax = 20
	}
	if resolved.Priorities.Interactive == 0 {
		resolved.Priorities.Interactive = 100
	}
	if resolved.Priorities.Forced == 0 {
		resolved.Priorities.Forced = 90
	}
	if resolved.Priorities.FreshIngest == 0 {
		resolved.Priorities.FreshIngest = 70
	}
	if resolved.Priorities.Repair == 0 {
		resolved.Priorities.Repair = 50
	}
	if resolved.Priorities.Background == 0 {
		resolved.Priorities.Background = 10
	}
	return resolved
}

func validateRabbitMQURL(rawURL, expectedVHost, field string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%s must be a valid amqp URL: %w", field, err)
	}
	if parsed.Scheme != "amqp" && parsed.Scheme != "amqps" {
		return fmt.Errorf("%s must use amqp or amqps scheme", field)
	}
	if parsed.Hostname() != "127.0.0.1" {
		return fmt.Errorf("%s must use host 127.0.0.1", field)
	}
	if strings.TrimPrefix(parsed.EscapedPath(), "/") != expectedVHost {
		return fmt.Errorf("%s must use RabbitMQ vhost /%s", field, expectedVHost)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
