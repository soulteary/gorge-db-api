package main

import (
	"fmt"
	"os"

	"github.com/soulteary/gorge-db-api/internal/cluster"
	"github.com/soulteary/gorge-db-api/internal/httpapi"
	"github.com/soulteary/gorge-db-api/internal/schema"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func main() {
	configPath := os.Getenv("PHORGE_CONFIG")
	var cfg *cluster.ClusterConfig
	if configPath != "" {
		var err error
		cfg, err = cluster.LoadFromFile(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = cluster.LoadFromEnv()
	}

	password := os.Getenv("MYSQL_PASS")

	healthSvc := cluster.NewHealthService(cfg)
	schemaSvc := schema.NewDiffService(cfg, password)
	setupSvc := schema.NewSetupService(cfg, password)
	migrationSvc := schema.NewMigrationService(cfg, password)

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	httpapi.RegisterRoutes(e, &httpapi.Deps{
		Health:    healthSvc,
		Schema:    schemaSvc,
		Setup:     setupSvc,
		Migration: migrationSvc,
		Password:  os.Getenv("SERVICE_TOKEN"),
	})

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	e.Logger.Fatal(e.Start(addr))
}
