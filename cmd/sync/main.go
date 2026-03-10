package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"inreview/internal/config"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
	"inreview/internal/worker"
)

func main() {
	_ = godotenv.Load()

	cfg := config.Load()
	if cfg.GitHubToken == "" {
		log.Println("WARNING: GITHUB_TOKEN not set — using unauthenticated API (60 req/hr limit)")
	}

	database, err := db.New(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	cache, err := rdb.New(cfg.RedisURL)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer cache.Close()

	ghClient := github.NewClient(cfg.GitHubToken)

	w := worker.New(ghClient, database, cache, cfg.GitHubAppID, cfg.GitHubAppPrivateKey)
	w.Start()
	log.Println("ngmi sync worker started")

	// Block until signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("ngmi sync worker shutting down")
}
