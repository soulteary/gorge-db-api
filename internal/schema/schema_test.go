package schema

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/soulteary/gorge-db-api/internal/cluster"
)

func TestNewDiffService(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "ns",
	}
	svc := NewDiffService(cfg, "pass")
	if svc == nil {
		t.Fatal("NewDiffService returned nil")
	}
	if svc.connFactory == nil {
		t.Error("connFactory should be set")
	}
}

func TestCollectRefsFiltersDisabled(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs: []*cluster.DatabaseRef{
			{Host: "h1"}, {Host: "h2", Disabled: true}, {Host: "h3"},
		},
	}
	svc := NewDiffService(cfg, "")
	refs := svc.CollectRefs()
	if len(refs) != 2 {
		t.Fatalf("expected 2, got %d", len(refs))
	}
}

func TestCollectRefsAllDisabled(t *testing.T) {
	cfg := &cluster.ClusterConfig{Refs: []*cluster.DatabaseRef{{Host: "h1", Disabled: true}}}
	svc := NewDiffService(cfg, "")
	if refs := svc.CollectRefs(); len(refs) != 0 {
		t.Errorf("expected 0, got %d", len(refs))
	}
}

func TestFlattenIssuesNoIssues(t *testing.T) {
	svc := &DiffService{}
	node := &SchemaNode{RefKey: "r", Status: "ok", Children: []*SchemaNode{
		{RefKey: "r", Database: "db1", Status: "ok"},
	}}
	var issues []SchemaIssue
	svc.flattenIssues(node, &issues)
	if len(issues) != 0 {
		t.Errorf("expected 0, got %d", len(issues))
	}
}

func TestFlattenIssuesWithIssues(t *testing.T) {
	svc := &DiffService{}
	node := &SchemaNode{
		RefKey: "r", Status: "warn", Issues: []string{"top"},
		Children: []*SchemaNode{
			{RefKey: "r", Database: "db", Table: "t1", Status: "fail", Issues: []string{"a", "b"}},
			{RefKey: "r", Database: "db", Table: "t2", Status: "ok"},
		},
	}
	var issues []SchemaIssue
	svc.flattenIssues(node, &issues)
	if len(issues) != 3 {
		t.Fatalf("expected 3, got %d", len(issues))
	}
}

func TestFlattenIssuesDeepNesting(t *testing.T) {
	svc := &DiffService{}
	node := &SchemaNode{RefKey: "r", Status: "ok", Children: []*SchemaNode{
		{RefKey: "r", Database: "db1", Status: "ok", Children: []*SchemaNode{
			{RefKey: "r", Database: "db1", Table: "t1", Status: "ok", Children: []*SchemaNode{
				{RefKey: "r", Database: "db1", Table: "t1", Column: "c1", Status: "warn", Issues: []string{"bad charset"}},
			}},
		}},
	}}
	var issues []SchemaIssue
	svc.flattenIssues(node, &issues)
	if len(issues) != 1 || issues[0].Column != "c1" {
		t.Errorf("unexpected: %v", issues)
	}
}

func TestLoadActualSchemaConnFail(t *testing.T) {
	cfg := &cluster.ClusterConfig{Namespace: "ns"}
	svc := &DiffService{config: cfg, connFactory: failConnFactory()}
	_, err := svc.LoadActualSchema(context.Background(), &cluster.DatabaseRef{Host: "h1", Port: 3306})
	if err == nil {
		t.Error("expected error")
	}
}

func TestLoadActualSchemaEmpty(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT SCHEMA_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"}))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{Namespace: "ns"}
	svc := &DiffService{config: cfg, connFactory: mockConnFactory(db)}
	node, err := svc.LoadActualSchema(context.Background(), &cluster.DatabaseRef{Host: "h1", Port: 3306})
	if err != nil {
		t.Fatal(err)
	}
	if node.Status != "ok" {
		t.Errorf("expected ok, got %q", node.Status)
	}
	if len(node.Children) != 0 {
		t.Errorf("expected 0 children, got %d", len(node.Children))
	}
}

func TestLoadActualSchemaWithDatabase(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT SCHEMA_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"}).
			AddRow("ns_config", "utf8mb4", "utf8mb4_unicode_ci"))
	// loadDatabaseSchema for ns_config
	mock.ExpectQuery("SELECT TABLE_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_COLLATION", "ENGINE"}).
			AddRow("users", "utf8mb4_unicode_ci", "InnoDB"))
	// columns for users
	cs := "utf8mb4"
	mock.ExpectQuery("SELECT COLUMN_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "CHARACTER_SET_NAME", "COLLATION_NAME"}).
			AddRow("id", "int", "NO", &cs, &cs).
			AddRow("name", "varchar(255)", "YES", &cs, &cs))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{Namespace: "ns"}
	svc := &DiffService{config: cfg, connFactory: mockConnFactory(db)}
	node, err := svc.LoadActualSchema(context.Background(), &cluster.DatabaseRef{Host: "h1", Port: 3306})
	if err != nil {
		t.Fatal(err)
	}
	if len(node.Children) != 1 {
		t.Fatalf("expected 1 db child, got %d", len(node.Children))
	}
	dbNode := node.Children[0]
	if dbNode.Database != "ns_config" {
		t.Errorf("expected ns_config, got %q", dbNode.Database)
	}
	if len(dbNode.Children) != 1 || dbNode.Children[0].Table != "users" {
		t.Errorf("expected 1 table 'users', got %v", dbNode.Children)
	}
	if len(dbNode.Children[0].Children) != 2 {
		t.Errorf("expected 2 columns, got %d", len(dbNode.Children[0].Children))
	}
}

func TestCollectIssuesConnFail(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "ns",
	}
	svc := &DiffService{config: cfg, connFactory: failConnFactory()}
	issues, err := svc.CollectIssues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].Status != "fail" {
		t.Errorf("expected 1 fail issue, got %v", issues)
	}
}

func TestCollectIssuesSkipsDisabled(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Disabled: true}},
		Namespace: "ns",
	}
	svc := &DiffService{config: cfg, connFactory: failConnFactory()}
	issues, err := svc.CollectIssues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Errorf("expected 0, got %d", len(issues))
	}
}

func TestCollectIssuesWithTree(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT SCHEMA_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"}))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "ns",
	}
	svc := &DiffService{config: cfg, connFactory: mockConnFactory(db)}
	issues, err := svc.CollectIssues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Errorf("expected 0, got %d", len(issues))
	}
}

func TestGetCharsetInfoConnFail(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "ns",
	}
	svc := &DiffService{config: cfg, connFactory: failConnFactory()}
	_, err := svc.GetCharsetInfo(context.Background())
	if err == nil {
		t.Error("expected error")
	}
}

func TestGetCharsetInfoUTF8MB4(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT CHARACTER_SET_NAME").WillReturnRows(
		sqlmock.NewRows([]string{"CHARACTER_SET_NAME"}).AddRow("utf8mb4"))
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "ns",
	}
	svc := &DiffService{config: cfg, connFactory: mockConnFactory(db)}
	infos, err := svc.GetCharsetInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1, got %d", len(infos))
	}
	if infos[0].CharsetDefault != "utf8mb4" {
		t.Errorf("expected utf8mb4, got %q", infos[0].CharsetDefault)
	}
	if infos[0].CollateText != "utf8mb4_bin" {
		t.Errorf("expected utf8mb4_bin, got %q", infos[0].CollateText)
	}
}

func TestGetCharsetInfoFallback(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT CHARACTER_SET_NAME").WillReturnError(sql.ErrNoRows)
	mock.ExpectClose()

	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Port: 3306}},
		Namespace: "ns",
	}
	svc := &DiffService{config: cfg, connFactory: mockConnFactory(db)}
	infos, err := svc.GetCharsetInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1, got %d", len(infos))
	}
	if infos[0].CharsetDefault != "binary" {
		t.Errorf("expected binary fallback, got %q", infos[0].CharsetDefault)
	}
}

func TestGetCharsetInfoSkipsDisabled(t *testing.T) {
	cfg := &cluster.ClusterConfig{
		Refs:      []*cluster.DatabaseRef{{Host: "h1", Disabled: true}},
		Namespace: "ns",
	}
	svc := &DiffService{config: cfg, connFactory: failConnFactory()}
	infos, err := svc.GetCharsetInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 0 {
		t.Errorf("expected 0, got %d", len(infos))
	}
}
