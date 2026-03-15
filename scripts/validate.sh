#!/usr/bin/env bash
# Integration validation script: compare Go API output with expected golden cases.
# Usage: ./scripts/validate.sh [GO_API_BASE_URL] [TOKEN]
#
# Requires: curl, jq

set -euo pipefail

BASE="${1:-http://localhost:8080}"
TOKEN="${2:-test-token}"

PASS=0
FAIL=0

check() {
    local name="$1"
    local endpoint="$2"
    local jq_expr="$3"
    local expected="$4"

    local resp
    resp=$(curl -sf -H "X-Service-Token: $TOKEN" "${BASE}${endpoint}" 2>/dev/null || echo '{"error":"connection_failed"}')

    local actual
    actual=$(echo "$resp" | jq -r "$jq_expr" 2>/dev/null || echo "PARSE_ERROR")

    if [ "$actual" = "$expected" ]; then
        echo "  PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $name"
        echo "    expected: $expected"
        echo "    actual:   $actual"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Phorge DB API Integration Validation ==="
echo "Base URL: $BASE"
echo ""

echo "--- Endpoint reachability ---"
check "GET /api/db/servers returns data" \
    "/api/db/servers" \
    '.data | type' \
    'array'

check "GET /api/db/schema-issues returns data" \
    "/api/db/schema-issues" \
    '.data | type' \
    'array'

check "GET /api/db/setup-issues returns data" \
    "/api/db/setup-issues" \
    '.data | type' \
    'array'

check "GET /api/db/migrations/status returns data" \
    "/api/db/migrations/status" \
    '.data | type' \
    'array'

echo ""
echo "--- Auth enforcement ---"
AUTH_RESP=$(curl -sf -o /dev/null -w "%{http_code}" "${BASE}/api/db/servers" 2>/dev/null || echo "000")
if [ "$AUTH_RESP" = "401" ]; then
    echo "  PASS: Missing token returns 401"
    PASS=$((PASS + 1))
else
    echo "  FAIL: Missing token should return 401, got $AUTH_RESP"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "--- Server status fields ---"
check "Server has connection_status field" \
    "/api/db/servers" \
    '.data[0].connection_status // "missing"' \
    'okay'

check "Server has connection_latency_sec" \
    "/api/db/servers" \
    '.data[0].connection_latency_sec | type' \
    'number'

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL
