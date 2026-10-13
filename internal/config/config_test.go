package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func clearEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		value, exists := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}

func writeConfig(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWorkerFileAndEnv(t *testing.T) {
	clearEnv(t, "DATABASE_URL", "WORKER_CONCURRENCY", "WORKER_LEASE", "WORKER_POLL")
	path := writeConfig(t, `[database]
url = "postgres://localhost/db"
[worker]
concurrency = 2
lease = "900ms"
poll = "20ms"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.Concurrency != 2 || cfg.Worker.Lease != 900*time.Millisecond ||
		cfg.Worker.Poll != 20*time.Millisecond {
		t.Fatal("TOML settings not loaded")
	}
	t.Setenv("WORKER_CONCURRENCY", "6")
	t.Setenv("WORKER_LEASE", "1m")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.Concurrency != 6 || cfg.Worker.Lease != time.Minute {
		t.Fatal("env override failed")
	}
}

func TestWorkerDefaultsAndFailures(t *testing.T) {
	clearEnv(t, "DATABASE_URL", "WORKER_CONCURRENCY", "WORKER_LEASE", "WORKER_POLL")
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.Concurrency != 4 || cfg.Worker.Lease != 30*time.Second || cfg.Worker.Poll != time.Second {
		t.Fatal("defaults not loaded")
	}
	for _, tc := range []struct{ name, key, value string }{
		{name: "zero concurrency", key: "WORKER_CONCURRENCY", value: "0"},
		{name: "invalid integer", key: "WORKER_CONCURRENCY", value: "bad"},
		{name: "short lease", key: "WORKER_LEASE", value: "1ms"},
		{name: "invalid duration", key: "WORKER_LEASE", value: "bad"},
		{name: "zero poll", key: "WORKER_POLL", value: "0s"},
		{name: "empty database", key: "DATABASE_URL", value: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(""); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
	if _, err := Load(writeConfig(t, "[worker]\nconcurrncy=2")); err == nil {
		t.Fatal("unknown key accepted")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.toml")); err == nil {
		t.Fatal("explicit missing file accepted")
	}
}

func TestFrameworkSnapshot(t *testing.T) {
	clearEnv(
		t,
		"DATABASE_URL",
		"WORKER_CONCURRENCY",
		"WORKER_LEASE",
		"WORKER_POLL",
		"LISTEN_ADDR",
		"GIN_MODE",
		"SERVER_CONFIG_PATH",
	)
	t.Setenv("TEST_WORKER_DSN", "postgres://localhost/db")
	t.Setenv("LISTEN_ADDR", "127.0.0.1:9101")
	t.Setenv("SERVER_CONFIG_PATH", writeConfig(t, `[database]
url = "${env:TEST_WORKER_DSN}"
[HTTPServer]
Listen = "127.0.0.1:9001"
`))
	cfg, manager, err := LoadManager("")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	var decoded Config
	if err := manager.Snapshot().Unmarshal(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Database != cfg.Database || decoded.Worker != cfg.Worker ||
		decoded.HTTPServer.Listen != cfg.HTTPServer.Listen ||
		cfg.HTTPServer.Listen != "127.0.0.1:9101" {
		t.Fatal("application and host snapshots differ")
	}
	for _, tc := range []struct{ key, value string }{
		{key: "LISTEN_ADDR", value: "bad"}, {key: "LISTEN_ADDR", value: "127.0.0.1:65536"}, {key: "GIN_MODE", value: "invalid"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, _, err := LoadManager(""); err == nil {
				t.Fatal("invalid framework setting accepted")
			}
		})
	}
}
