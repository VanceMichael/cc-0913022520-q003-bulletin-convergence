//go:build integration

// 集成测试使用真实 PostgreSQL 与真实 HTTP 回调，覆盖：
// 并发发布、投递器崩溃/重启恢复、分级重试、重复回执、撤销后迟到确认，
// 并从公告详情与 Prometheus 指标三方对账。
//
// 运行方式（二选一）：
//
//	# 1) 指向外部 PostgreSQL 14+（每次测试使用独立 schema，结束自动清理）
//	TEST_DATABASE_URL=postgres://user:pass@localhost:5432/postgres?sslmode=disable \
//	  go test -tags=integration -v ./test/integration
//
//	# 2) 使用解压好的便携 PostgreSQL（目录内有 bin/postgres）
//	TEST_PG_HOME=/tmp/pgbin/pg go test -tags=integration -v ./test/integration
package integration_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/disruption-bulletins/internal/store"
)

// startPostgres 返回测试用数据库连接串与清理函数。优先使用 TEST_DATABASE_URL
// 指向的外部实例（每个测试进程隔离到独立 schema），否则启动便携 PostgreSQL。
func startPostgres(t *testing.T) (string, func()) {
	t.Helper()

	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		return startExternalSchema(t, url)
	}

	home := os.Getenv("TEST_PG_HOME")
	if home == "" {
		home = filepath.Join(os.TempDir(), "pgbin", "pg")
	}
	if _, err := os.Stat(filepath.Join(home, "bin", "postgres")); err != nil {
		t.Skipf("integration tests need TEST_DATABASE_URL or TEST_PG_HOME (portable postgres dir): %v", err)
	}
	return startPortable(t, home)
}

var schemaCounter int64

func startExternalSchema(t *testing.T, baseURL string) (string, func()) {
	ctx := context.Background()
	n := atomic.AddInt64(&schemaCounter, 1)
	schema := "it_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_" + strconv.FormatInt(n, 10)

	admin, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatalf("connect external postgres: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	admin.Close()

	cfg, err := pgx.ParseConfig(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams = map[string]string{"search_path": schema}
	// pgxpool 不直接接受 pgx.ConnConfig 字符串，用 DSN 追加 search_path。
	out := baseURL
	if sep := "?"; contains(out, "?") {
		sep = "&"
		out = out + sep + "search_path=" + schema
	} else {
		out = out + "?search_path=" + schema
	}
	_ = cfg

	cleanup := func() {
		// 用新连接删除，避免 search_path 残留。
		p, err := pgxpool.New(context.Background(), baseURL)
		if err == nil {
			_, _ = p.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
			p.Close()
		}
	}
	return out, cleanup
}

func contains(s string, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func startPortable(t *testing.T, home string) (string, func()) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	sockDir := filepath.Join(dir, "sock")
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		t.Fatal(err)
	}

	port := freePort(t)
	bin := func(name string) string { return filepath.Join(home, "bin", name) }
	libPath := filepath.Join(home, "lib")

	initdb := exec.Command(bin("initdb"), "-D", dataDir, "-U", "postgres",
		"--auth=trust", "--encoding=UTF8", "--no-locale")
	initdb.Env = append(os.Environ(), "LD_LIBRARY_PATH="+libPath)
	if out, err := initdb.CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v\n%s", err, out)
	}

	logFile := filepath.Join(dir, "postgres.log")
	start := exec.Command(bin("pg_ctl"), "-D", dataDir, "-l", logFile, "-w", "-t", "30", "start",
		"-o", fmt.Sprintf(
			"-c listen_addresses=127.0.0.1 -p %d -k %s -c fsync=off -c full_page_writes=off -c synchronous_commit=off",
			port, sockDir))
	start.Env = append(os.Environ(), "LD_LIBRARY_PATH="+libPath)
	if out, err := start.CombinedOutput(); err != nil {
		t.Fatalf("pg_ctl start: %v\n%s\n%s", err, out, readFileQuiet(logFile))
	}

	url := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	cleanup := func() {
		stop := exec.Command(bin("pg_ctl"), "-D", dataDir, "-m", "fast", "-w", "stop")
		stop.Env = append(os.Environ(), "LD_LIBRARY_PATH="+libPath)
		_ = stop.Run()
	}
	return url, cleanup
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func readFileQuiet(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}

// migrateOpen 建池、迁移并返回池。
func migrateOpen(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
