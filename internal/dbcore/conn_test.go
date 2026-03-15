package dbcore

import "testing"

func TestIsReadQuery(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"SELECT * FROM foo", true},
		{"  SELECT 1", true},
		{"(SELECT a) UNION (SELECT b)", true},
		{"SHOW TABLES", true},
		{"EXPLAIN SELECT 1", true},
		{"INSERT INTO foo VALUES (1)", false},
		{"UPDATE foo SET a=1", false},
		{"DELETE FROM foo", false},
		{"CREATE TABLE foo (id INT)", false},
		{"DROP TABLE foo", false},
		{"START TRANSACTION", false},
		{"COMMIT", false},
	}
	for _, tc := range cases {
		got := isReadQuery(tc.query)
		if got != tc.want {
			t.Errorf("isReadQuery(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}
