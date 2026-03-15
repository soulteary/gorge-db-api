package dbcore

import "regexp"

var readQueryRe = regexp.MustCompile(`(?i)^\s*\(?\s*(SELECT|SHOW|EXPLAIN)\s`)

func isReadQuery(q string) bool {
	return readQueryRe.MatchString(q)
}
