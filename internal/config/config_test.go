package config

import (
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
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
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
	if cfg.Worker.Concurrency != 2 || cfg.Worker.Lease != 900*time.Millisecond || cfg.Worker.Poll != 20*time.Millisecond {
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
		{"zero concurrency", "WORKER_CONCURRENCY", "0"}, {"invalid integer", "WORKER_CONCURRENCY", "bad"},
		{"short lease", "WORKER_LEASE", "1ms"}, {"invalid duration", "WORKER_LEASE", "bad"},
		{"zero poll", "WORKER_POLL", "0s"}, {"empty database", "DATABASE_URL", ""},
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
