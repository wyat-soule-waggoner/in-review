package main

import (
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"

	"inreview/internal/analytics"
	"inreview/internal/config"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/handlers"
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
	ph := analytics.New(cfg.PostHogAPIKey)
	defer ph.Close()

	// Create the worker for queue operations (Queue/IsSyncing/QueuePosition)
	// but do NOT call Start() — sync goroutines run in the sync binary.
	q := worker.New(ghClient, database, cache, cfg.GitHubAppID, cfg.GitHubAppPrivateKey)

	h := handlers.New(database, ghClient, q, cache, cfg, ph)
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(middleware.Compress(5))
	r.Use(cache.RateLimit(300, time.Minute))
	r.Use(h.SessionLoader)
	r.Use(h.TrackPageViews)

	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	r.Get("/", h.Home)
	r.Get("/stats", h.Stats)
	r.Get("/search", h.Search)
	r.Get("/repo/{owner}/{name}", h.Repo)
	r.Get("/user/{username}", h.User)
	r.Get("/org/{org}", h.Org)

	r.Get("/badge/{owner}/{name}.svg", h.Badge)

	r.Get("/leaderboard/{category}", h.LeaderboardPage)
	r.Get("/leaderboard/{category}/rows", h.LeaderboardRows)
	r.Get("/leaderboard/{category}/search", h.LeaderboardSearch)
	r.Get("/api/leaderboard", h.LeaderboardAPI)
	r.Get("/api/repo/{owner}/{name}/charts", h.RepoCharts)
	r.Get("/api/user/{username}/charts", h.UserCharts)
	r.Post("/api/sync/{owner}/{name}", h.TriggerSync)
	r.Get("/api/sync-status/{owner}/{name}", h.SyncStatus)
	r.Get("/hi-wall", h.HiWall)
	r.Get("/api/hi", h.HiGet)
	r.Post("/api/hi", h.HiPost)

	r.Get("/data", h.DataExplorer)
	r.Get("/data/repos", h.DataRepos)
	r.Get("/data/prs", h.DataPRs)
	r.Get("/data/reviews", h.DataReviews)
	r.Get("/data/users", h.DataUsers)

	r.Get("/blog", h.Blog)
	r.Get("/api/blog/stats", h.BlogLiveStats)

	r.Get("/auth/login", h.AuthLogin)
	r.Get("/auth/github", h.AuthGitHub)
	r.Get("/auth/github/callback", h.AuthGitHubCallback)
	r.Post("/auth/logout", h.AuthLogout)
	r.Post("/api/github/webhook", h.GitHubWebhook)

	r.Get("/dashboard", h.RequireAuth(h.Dashboard))
	r.Post("/api/repos/add", h.RequireAuth(h.AddRepo))

	// Rebuild materialized leaderboard tables and keep them fresh.
	// Also warms the home page Redis cache. Prevents 30+ second cold-cache hits.
	go h.WarmLeaderboards()

	log.Printf("ngmi web listening on http://localhost:%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, r))
}
