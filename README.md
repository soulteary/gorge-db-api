# Phorge Database API (Go + Echo)

A Go service providing Phorge-compatible database management HTTP APIs, implementing
the same database capabilities as Phorge's PHP infrastructure layer.

## Architecture

```
cmd/server/         - Echo server entry point
internal/
  compat/           - Error codes, HTTP response envelope (Phorge-compatible)
  dbcore/           - Connection pool, read/write enforcement, transactions, retry
  cluster/          - Master/replica selection, health probing, config parsing
  schema/           - Schema diff, setup issues, migration status
  httpapi/          - REST handlers and route registration
```

## Quick Start

```bash
# Environment variables
export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASS=your_password
export SERVICE_TOKEN=your_service_token
export STORAGE_NAMESPACE=phorge

# Build and run
go build -o github.com/soulteary/gorge-db-api ./cmd/server
./github.com/soulteary/gorge-db-api

# Or with a Phorge config file
export PHORGE_CONFIG=/path/to/local.json
./github.com/soulteary/gorge-db-api
```

## API Endpoints

All endpoints require `X-Service-Token` header.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/db/servers` | List all DB nodes with health status |
| GET | `/api/db/servers/:ref/health` | Single node health details |
| GET | `/api/db/schema-diff` | Schema tree from INFORMATION_SCHEMA |
| GET | `/api/db/schema-issues` | Flattened schema issues |
| GET | `/api/db/setup-issues` | Database setup check issues |
| GET | `/api/db/migrations/status` | Patch application status |

See [docs/db-migration/http-contract.md](../docs/db-migration/http-contract.md) for full API spec.

## Testing

```bash
go test ./...

# Integration validation (requires running server + MySQL)
./scripts/validate.sh http://localhost:8080 your_service_token
```

## Behavior Spec

The implementation follows [docs/db-migration/behavior-spec.md](../docs/db-migration/behavior-spec.md)
which documents the exact Phorge PHP behavior this service replicates.
