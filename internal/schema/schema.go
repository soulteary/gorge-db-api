package schema

import (
	"context"
	"fmt"

	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/dbcore"
)

// SchemaNode mirrors PhabricatorConfigSchemaQuery's hierarchical output.
type SchemaNode struct {
	RefKey   string        `json:"ref_key"`
	Database string        `json:"database,omitempty"`
	Table    string        `json:"table,omitempty"`
	Column   string        `json:"column,omitempty"`
	Key      string        `json:"key,omitempty"`
	Issues   []string      `json:"issues,omitempty"`
	Status   string        `json:"status"` // ok, warn, fail
	Children []*SchemaNode `json:"children,omitempty"`
}

// SchemaIssue is a flattened schema problem for the issues endpoint.
type SchemaIssue struct {
	RefKey   string `json:"ref_key"`
	Database string `json:"database"`
	Table    string `json:"table,omitempty"`
	Column   string `json:"column,omitempty"`
	Key      string `json:"key,omitempty"`
	Issue    string `json:"issue"`
	Status   string `json:"status"`
}

type DiffService struct {
	config   *cluster.ClusterConfig
	password string
}

func NewDiffService(config *cluster.ClusterConfig, password string) *DiffService {
	return &DiffService{config: config, password: password}
}

// LoadActualSchema queries INFORMATION_SCHEMA for all databases belonging
// to the configured namespace, mirroring PhabricatorConfigSchemaQuery::loadActualSchemaForServer.
func (s *DiffService) LoadActualSchema(ctx context.Context, ref *cluster.DatabaseRef) (*SchemaNode, error) {
	dsn := dbcore.DSN{
		Host:            ref.Host,
		Port:            ref.Port,
		User:            ref.User,
		Password:        s.password,
		ConnTimeoutSec:  2,
		QueryTimeoutSec: 30,
	}
	conn, err := dbcore.NewConn(dsn, true)
	if err != nil {
		return nil, fmt.Errorf("connect for schema: %w", err)
	}
	defer func() { _ = conn.Close() }()

	server := &SchemaNode{
		RefKey: ref.RefKey(),
		Status: "ok",
	}

	prefix := s.config.Namespace + "_%"
	rows, err := conn.QueryContext(ctx,
		"SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME "+
			"FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE ?", prefix)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var databases []string
	for rows.Next() {
		var name, charset, collation string
		if err := rows.Scan(&name, &charset, &collation); err != nil {
			continue
		}
		databases = append(databases, name)
	}

	for _, dbName := range databases {
		dbNode, err := s.loadDatabaseSchema(ctx, conn, ref.RefKey(), dbName)
		if err != nil {
			dbNode = &SchemaNode{RefKey: ref.RefKey(), Database: dbName, Status: "fail", Issues: []string{err.Error()}}
		}
		server.Children = append(server.Children, dbNode)
	}

	return server, nil
}

func (s *DiffService) loadDatabaseSchema(ctx context.Context, conn *dbcore.Conn, refKey, dbName string) (*SchemaNode, error) {
	dbNode := &SchemaNode{
		RefKey:   refKey,
		Database: dbName,
		Status:   "ok",
	}

	rows, err := conn.QueryContext(ctx,
		"SELECT TABLE_NAME, TABLE_COLLATION, ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?",
		dbName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, collation, engine string
		if err := rows.Scan(&tableName, &collation, &engine); err != nil {
			continue
		}
		tableNode := &SchemaNode{
			RefKey:   refKey,
			Database: dbName,
			Table:    tableName,
			Status:   "ok",
		}
		colRows, err := conn.QueryContext(ctx,
			"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, COLLATION_NAME "+
				"FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			dbName, tableName)
		if err == nil {
			for colRows.Next() {
				var colName, colType, nullable string
				var charset, colCollation *string
				if err := colRows.Scan(&colName, &colType, &nullable, &charset, &colCollation); err != nil {
					continue
				}
				tableNode.Children = append(tableNode.Children, &SchemaNode{
					RefKey:   refKey,
					Database: dbName,
					Table:    tableName,
					Column:   colName,
					Status:   "ok",
				})
			}
			_ = colRows.Close()
		}

		dbNode.Children = append(dbNode.Children, tableNode)
	}

	return dbNode, nil
}

type CharsetInfo struct {
	RefKey         string `json:"ref_key"`
	CharsetDefault string `json:"charset_default"`
	CharsetSort    string `json:"charset_sort"`
	CharsetFull    string `json:"charset_fulltext"`
	CollateText    string `json:"collate_text"`
	CollateSort    string `json:"collate_sort"`
	CollateFull    string `json:"collate_fulltext"`
}

func (s *DiffService) GetCharsetInfo(ctx context.Context) ([]CharsetInfo, error) {
	var results []CharsetInfo
	for _, ref := range s.config.GetAllRefs() {
		if ref.Disabled {
			continue
		}
		info, err := s.charsetInfoForRef(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("charset info for %s: %w", ref.RefKey(), err)
		}
		results = append(results, *info)
	}
	return results, nil
}

func (s *DiffService) charsetInfoForRef(ctx context.Context, ref *cluster.DatabaseRef) (*CharsetInfo, error) {
	dsn := dbcore.DSN{
		Host:            ref.Host,
		Port:            ref.Port,
		User:            ref.User,
		Password:        s.password,
		ConnTimeoutSec:  2,
		QueryTimeoutSec: 10,
	}
	conn, err := dbcore.NewConn(dsn, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	var name string
	row := conn.QueryRowContext(ctx,
		"SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'")
	hasUTF8MB4 := row.Scan(&name) == nil

	info := &CharsetInfo{RefKey: ref.RefKey()}
	if hasUTF8MB4 {
		info.CharsetDefault = "utf8mb4"
		info.CharsetSort = "utf8mb4"
		info.CharsetFull = "utf8mb4"
		info.CollateText = "utf8mb4_bin"
		info.CollateSort = "utf8mb4_unicode_ci"
		info.CollateFull = "utf8mb4_unicode_ci"
	} else {
		info.CharsetDefault = "binary"
		info.CharsetSort = "utf8"
		info.CharsetFull = "utf8"
		info.CollateText = "binary"
		info.CollateSort = "utf8_general_ci"
		info.CollateFull = "utf8_general_ci"
	}
	return info, nil
}

func (s *DiffService) CollectRefs() []*cluster.DatabaseRef {
	var refs []*cluster.DatabaseRef
	for _, ref := range s.config.GetAllRefs() {
		if !ref.Disabled {
			refs = append(refs, ref)
		}
	}
	return refs
}

// CollectIssues scans all refs and returns flattened schema issues.
func (s *DiffService) CollectIssues(ctx context.Context) ([]SchemaIssue, error) {
	var issues []SchemaIssue
	for _, ref := range s.config.GetAllRefs() {
		if ref.Disabled {
			continue
		}
		tree, err := s.LoadActualSchema(ctx, ref)
		if err != nil {
			issues = append(issues, SchemaIssue{
				RefKey: ref.RefKey(),
				Issue:  err.Error(),
				Status: "fail",
			})
			continue
		}
		s.flattenIssues(tree, &issues)
	}
	return issues, nil
}

func (s *DiffService) flattenIssues(node *SchemaNode, out *[]SchemaIssue) {
	for _, issue := range node.Issues {
		*out = append(*out, SchemaIssue{
			RefKey:   node.RefKey,
			Database: node.Database,
			Table:    node.Table,
			Column:   node.Column,
			Key:      node.Key,
			Issue:    issue,
			Status:   node.Status,
		})
	}
	for _, child := range node.Children {
		s.flattenIssues(child, out)
	}
}
