package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/server-go/conf"
)

func writeConfig(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func decodeConfig(t *testing.T, path string) (Config, error) {
	t.Helper()
	snapshot, err := conf.Load(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := snapshot.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

func TestFrameworkConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("GIN_MODE", "release")
	t.Setenv("LISTEN_ADDR", "127.0.0.1:9101")
	t.Setenv("WORKER_CONCURRENCY", "6")
	t.Setenv("WORKER_LEASE", "900ms")
	t.Setenv("WORKER_POLL", "20ms")
	path := filepath.Join("..", "..", "conf", "server", "config.toml")
	cfg, err := decodeConfig(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DB.DSN != "postgres://localhost/db" || cfg.HTTPServer.Listen != "127.0.0.1:9101" ||
		cfg.Worker.Concurrency != 6 || cfg.Worker.Lease != 900*time.Millisecond || cfg.Worker.Poll != 20*time.Millisecond {
		t.Fatalf("configuration not decoded: %+v", cfg)
	}
	if cfg.DB.MaxOpenConns != 20 || cfg.DB.MaxIdleConns != 5 || cfg.DB.AutoMigrate {
		t.Fatal("framework database configuration not decoded")
	}
}

func TestFrameworkSnapshot(t *testing.T) {
	t.Setenv("TEST_WORKER_DSN", "postgres://localhost/first")
	path := writeConfig(t, `AppName = "worker-test"
AppCode = 101
RunMode = "test"
[HTTPServer]
Listen = "127.0.0.1:0"
[DB]
Driver = "PostgreSQL"
DSN = "${env:TEST_WORKER_DSN}"
[Worker]
Concurrency = 4
Lease = "30s"
Poll = "1s"
`)
	t.Setenv("SERVER_CONFIG_PATH", path)
	snapshot, err := conf.Load("")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_WORKER_DSN", "postgres://localhost/second")
	var cfg Config
	if err := snapshot.Unmarshal(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.DB.DSN != "postgres://localhost/first" || cfg.Worker.Concurrency != 4 ||
		cfg.Worker.Lease != 30*time.Second || cfg.Worker.Poll != time.Second {
		t.Fatal("startup snapshot changed or duration conversion failed")
	}
}

func TestInvalidConfiguration(t *testing.T) {
	base := `AppName = "worker-test"
AppCode = 101
RunMode = "test"
[HTTPServer]
Listen = "127.0.0.1:0"
[DB]
Driver = "PostgreSQL"
DSN = "postgres://localhost/db"
AutoMigrate = false
[Worker]
Concurrency = 4
Lease = "30s"
Poll = "1s"
`
	for _, tc := range []struct {
		name, old, value, want string
	}{
		{name: "wrong driver", old: `Driver = "PostgreSQL"`, value: `Driver = "SQLite"`, want: "DB.Driver"},
		{name: "auto migration", old: "AutoMigrate = false", value: "AutoMigrate = true", want: "DB.AutoMigrate"},
		{name: "missing database", old: `DSN = "postgres://localhost/db"`, value: `DSN = ""`, want: "DB.DSN"},
		{name: "invalid database", old: `DSN = "postgres://localhost/db"`, value: `DSN = "postgres://user:secret@host:bad/db"`, want: "invalid DB.DSN"},
		{name: "zero concurrency", old: "Concurrency = 4", value: "Concurrency = 0", want: "Worker.Concurrency"},
		{name: "invalid integer", old: "Concurrency = 4", value: `Concurrency = "bad"`, want: "Concurrency"},
		{name: "short lease", old: `Lease = "30s"`, value: `Lease = "1ms"`, want: "Worker.Lease"},
		{name: "invalid duration", old: `Lease = "30s"`, value: `Lease = "bad"`, want: "Lease"},
		{name: "zero poll", old: `Poll = "1s"`, value: `Poll = "0s"`, want: "Worker.Poll"},
		{name: "invalid listen", old: `Listen = "127.0.0.1:0"`, value: `Listen = "bad"`, want: "HTTPServer.Listen"},
		{name: "invalid port", old: `Listen = "127.0.0.1:0"`, value: `Listen = "127.0.0.1:65536"`, want: "HTTPServer.Listen"},
		{name: "invalid run mode", old: `RunMode = "test"`, value: `RunMode = "invalid"`, want: "RunMode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeConfig(t, writeConfig(t, strings.Replace(base, tc.old, tc.value, 1)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %s configuration error, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("configuration error exposed database credentials")
			}
		})
	}
	if _, err := decodeConfig(t, filepath.Join(t.TempDir(), "missing.toml")); err == nil {
		t.Fatal("explicit missing file accepted")
	}
}
