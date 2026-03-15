package httpapi

import (
	"net/http"

	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/compat"
	"github.com/soulteary/gorge-db-api/internal/schema"

	"github.com/labstack/echo/v4"
)

type Deps struct {
	Health    *cluster.HealthService
	Schema    *schema.DiffService
	Setup     *schema.SetupService
	Migration *schema.MigrationService
	Password  string
}

func RegisterRoutes(e *echo.Echo, deps *Deps) {
	e.GET("/", healthPing())
	e.GET("/healthz", healthPing())

	g := e.Group("/api/db")
	g.Use(tokenAuth(deps))

	g.GET("/servers", listServers(deps))
	g.GET("/servers/:ref/health", serverHealth(deps))
	g.GET("/schema-diff", schemaDiff(deps))
	g.GET("/schema-issues", schemaIssues(deps))
	g.GET("/setup-issues", setupIssues(deps))
	g.GET("/charset-info", charsetInfo(deps))
	g.GET("/migrations/status", migrationStatus(deps))
}

func tokenAuth(deps *Deps) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			token := c.Request().Header.Get("X-Service-Token")
			if token == "" {
				token = c.QueryParam("token")
			}
			if token == "" {
				return c.JSON(http.StatusUnauthorized, &compat.APIResponse{
					Error: &compat.APIError{Code: "ERR_UNAUTHORIZED", Message: "missing service token"},
				})
			}
			return next(c)
		}
	}
}

func healthPing() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	}
}

func listServers(deps *Deps) echo.HandlerFunc {
	return func(c echo.Context) error {
		refs, err := deps.Health.QueryAll(c.Request().Context(), deps.Password)
		if err != nil {
			return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
		}
		return compat.RespondOK(c, refs)
	}
}

func serverHealth(deps *Deps) echo.HandlerFunc {
	return func(c echo.Context) error {
		refKey := c.Param("ref")
		ref, err := deps.Health.QueryOne(c.Request().Context(), refKey, deps.Password)
		if err != nil {
			return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
		}
		return compat.RespondOK(c, ref)
	}
}

func schemaDiff(deps *Deps) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		var nodes []*schema.SchemaNode
		for _, ref := range deps.Schema.CollectRefs() {
			tree, err := deps.Schema.LoadActualSchema(ctx, ref)
			if err != nil {
				return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
			}
			nodes = append(nodes, tree)
		}
		return compat.RespondOK(c, nodes)
	}
}

func schemaIssues(deps *Deps) echo.HandlerFunc {
	return func(c echo.Context) error {
		issues, err := deps.Schema.CollectIssues(c.Request().Context())
		if err != nil {
			return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
		}
		return compat.RespondOK(c, issues)
	}
}

func setupIssues(deps *Deps) echo.HandlerFunc {
	return func(c echo.Context) error {
		issues, err := deps.Setup.CollectIssues(c.Request().Context())
		if err != nil {
			return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
		}
		return compat.RespondOK(c, issues)
	}
}

func charsetInfo(deps *Deps) echo.HandlerFunc {
	return func(c echo.Context) error {
		info, err := deps.Schema.GetCharsetInfo(c.Request().Context())
		if err != nil {
			return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
		}
		return compat.RespondOK(c, info)
	}
}

func migrationStatus(deps *Deps) echo.HandlerFunc {
	return func(c echo.Context) error {
		statuses, err := deps.Migration.Status(c.Request().Context())
		if err != nil {
			return compat.RespondError(c, compat.NewClusterError(compat.ErrQuery, err.Error()))
		}
		return compat.RespondOK(c, statuses)
	}
}
