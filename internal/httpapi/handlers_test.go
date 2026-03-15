package httpapi

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/compat"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
	"github.com/soulteary/gorge-db-api/internal/schema"

	"github.com/labstack/echo/v4"
)

func mockFactory(db *sql.DB) dbcore.ConnFactory {
	return func(dsn dbcore.DSN, readOnly bool) (*dbcore.Conn, error) {
		return dbcore.NewConnFromDB(db, dsn, readOnly), nil
	}
}

func newMockDeps(t *testing.T) (*Deps, *sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	cfg := cluster.BuildConfig(cluster.RawConfig{
		MysqlHost: "localhost", MysqlPort: 3306, MysqlUser: "root", Namespace: "phorge",
	})

	factory := mockFactory(db)
	healthSvc := cluster.NewHealthService(cfg)
	healthSvc.SetConnFactory(factory)
	schemaSvc := schema.NewDiffService(cfg, "")
	schemaSvc.SetConnFactory(factory)
	setupSvc := schema.NewSetupService(cfg, "")
	setupSvc.SetConnFactory(factory)
	migrationSvc := schema.NewMigrationService(cfg, "")
	migrationSvc.SetConnFactory(factory)

	return &Deps{
		Health: healthSvc, Schema: schemaSvc,
		Setup: setupSvc, Migration: migrationSvc,
		Password: "test-token",
	}, db, mock
}

func authedReq(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-Service-Token", "test-token")
	return req
}

func TestHealthPing(t *testing.T) {
	e := echo.New()
	deps, db, _ := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Errorf("expected ok, got %q", body["status"])
	}
}

func TestHealthPingRoot(t *testing.T) {
	e := echo.New()
	deps, db, _ := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestTokenAuthMissing(t *testing.T) {
	e := echo.New()
	deps, db, _ := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/db/servers", nil))
	if rec.Code != 401 {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestTokenAuthHeader(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/servers"))
	if rec.Code == 401 {
		t.Error("should not be 401 with token")
	}
}

func TestTokenAuthQueryParam(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/db/servers?token=test-token", nil))
	if rec.Code == 401 {
		t.Error("should not be 401 with query param token")
	}
}

func TestListServers(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/servers"))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp compat.APIResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error != nil {
		t.Errorf("unexpected error: %+v", resp.Error)
	}
	if resp.Data == nil {
		t.Error("data should not be nil")
	}
}

func TestServerHealthFound(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"col1"}))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/servers/localhost:3306/health"))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestServerHealthNotFound(t *testing.T) {
	e := echo.New()
	deps, db, _ := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/servers/nonexist:9999/health"))
	if rec.Code != 500 {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestSetupIssues(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectPing()
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("8.0.33"))
	mock.ExpectQuery("SHOW ENGINES").WillReturnRows(
		sqlmock.NewRows([]string{"Engine", "Support", "c1", "c2", "c3", "c4"}).
			AddRow("InnoDB", "DEFAULT", nil, nil, nil, nil))
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(
		sqlmock.NewRows([]string{"Database"}).AddRow("phorge_meta_data"))
	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(int64(64 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@sql_mode").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("STRICT_ALL_TABLES"))
	mock.ExpectQuery("SELECT @@innodb_buffer_pool_size").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(int64(512 * 1024 * 1024)))
	mock.ExpectQuery("SELECT @@local_infile").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(0))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(int64(9999999999)))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/setup-issues"))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestMigrationStatus(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectPing()
	mock.ExpectQuery("SELECT patch FROM patch_status").WillReturnRows(
		sqlmock.NewRows([]string{"patch"}).AddRow("001.sql"))
	mock.ExpectQuery("SELECT stateValue FROM hoststate").WillReturnRows(
		sqlmock.NewRows([]string{"stateValue"}))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/migrations/status"))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestSchemaIssues(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectQuery("SELECT SCHEMA_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"}))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/schema-issues"))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestSchemaDiff(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectQuery("SELECT SCHEMA_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"}))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/schema-diff"))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestCharsetInfo(t *testing.T) {
	e := echo.New()
	deps, db, mock := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	mock.ExpectQuery("SELECT CHARACTER_SET_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"CHARACTER_SET_NAME"}).AddRow("utf8mb4"))
	mock.ExpectClose()

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/api/db/charset-info"))
	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRegisterRoutesAllEndpoints(t *testing.T) {
	e := echo.New()
	deps, db, _ := newMockDeps(t)
	defer db.Close()
	RegisterRoutes(e, deps)

	paths := make(map[string]bool)
	for _, r := range e.Routes() {
		paths[r.Path] = true
	}
	expected := []string{
		"/", "/healthz",
		"/api/db/servers", "/api/db/servers/:ref/health",
		"/api/db/schema-diff", "/api/db/schema-issues",
		"/api/db/setup-issues", "/api/db/charset-info",
		"/api/db/migrations/status",
	}
	for _, p := range expected {
		if !paths[p] {
			t.Errorf("missing route %q", p)
		}
	}
}
