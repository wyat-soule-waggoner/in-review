package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"inreview/internal/db"
)

type HomeData struct {
	BaseData
	TotalRepos      int
	TotalPRs        int
	TotalReviews    int
	SpeedDemons     []db.LeaderboardEntry
	PRGraveyard     []db.LeaderboardEntry
	ReviewChamps    []db.LeaderboardEntry
	Gatekeepers     []db.LeaderboardEntry
	MergeMasters    []db.LeaderboardEntry
	OneShot         []db.LeaderboardEntry
	PopularVisits   []db.PageVisit
	RecentVisits    []db.PageVisit
	OGTitle         string
	OGDesc          string
	OGUrl           string
}

// homeLBCache holds all data needed to render the home page, cached together
// to eliminate all DB queries on cache hit.
type homeLBCache struct {
	SpeedDemons   []db.LeaderboardEntry
	PRGraveyard   []db.LeaderboardEntry
	ReviewChamps  []db.LeaderboardEntry
	Gatekeepers   []db.LeaderboardEntry
	MergeMasters  []db.LeaderboardEntry
	OneShot       []db.LeaderboardEntry
	TotalRepos    int
	TotalPRs      int
	TotalReviews  int
	PopularVisits []db.PageVisit
	RecentVisits  []db.PageVisit
}

const homeLBCacheKey = "home:lb"
const homeLBCacheTTL = 15 * time.Minute

// homeSF deduplicates concurrent cache-miss rebuilds so only one set of
// leaderboard queries runs even when many requests arrive simultaneously.
var homeSF singleflight.Group

// buildHomeCache runs all leaderboard queries and stores the result in Redis.
func (h *Handler) buildHomeCache(ctx context.Context) (homeLBCache, error) {
	v, err, _ := homeSF.Do(homeLBCacheKey, func() (interface{}, error) {
		var lb homeLBCache
		var wg sync.WaitGroup
		wg.Add(8)
		go func() { defer wg.Done(); lb.SpeedDemons, _ = h.db.LeaderboardReposBySpeed("ASC", 5) }()
		go func() { defer wg.Done(); lb.PRGraveyard, _ = h.db.LeaderboardReposBySpeed("DESC", 5) }()
		go func() { defer wg.Done(); lb.ReviewChamps, _ = h.db.LeaderboardReviewers(5) }()
		go func() { defer wg.Done(); lb.Gatekeepers, _ = h.db.LeaderboardGatekeepers(5) }()
		go func() { defer wg.Done(); lb.MergeMasters, _ = h.db.LeaderboardAuthors(5) }()
		go func() { defer wg.Done(); lb.OneShot, _ = h.db.LeaderboardCleanApprovals(5) }()
		go func() { defer wg.Done(); lb.TotalRepos, lb.TotalPRs, lb.TotalReviews = h.db.TotalStats() }()
		go func() {
			defer wg.Done()
			pop, _ := h.db.PopularVisits(3)
			lb.PopularVisits = pop
			if len(pop) > 0 {
				exclude := make([]string, len(pop))
				for i, v := range pop {
					exclude[i] = v.Path
				}
				lb.RecentVisits, _ = h.db.RecentVisits(5, exclude)
			} else {
				lb.RecentVisits, _ = h.db.RecentVisits(5, nil)
			}
		}()
		wg.Wait()

		if raw, err := json.Marshal(lb); err == nil {
			h.cache.Set(ctx, homeLBCacheKey, raw, homeLBCacheTTL)
		}
		return lb, nil
	})
	if err != nil {
		return homeLBCache{}, err
	}
	return v.(homeLBCache), nil
}

// WarmLeaderboards rebuilds the materialized leaderboard tables then keeps
// them fresh on a timer. The home page Redis cache is also refreshed afterwards
// so it immediately reflects the new data. Call once at startup in a goroutine.
func (h *Handler) WarmLeaderboards() {
	ctx := context.Background()

	rebuild := func() {
		log.Printf("leaderboards: refreshing materialized tables…")
		if err := h.db.RefreshLeaderboards(); err != nil {
			log.Printf("leaderboards: refresh error: %v", err)
			return
		}
		log.Printf("leaderboards: materialized tables ready, warming home cache…")
		// Invalidate the home Redis cache so the next rebuild reads fresh mat data.
		h.cache.Del(ctx, homeLBCacheKey)
		if _, err := h.buildHomeCache(ctx); err != nil {
			log.Printf("leaderboards: home cache warm error: %v", err)
		} else {
			log.Printf("leaderboards: home cache ready")
		}
	}

	rebuild()

	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rebuild()
	}
}

// WarmHomeCache pre-builds the home leaderboard cache and then refreshes it
// on a timer so it is never cold during normal operation. Call once at startup
// in a goroutine. Note: if WarmLeaderboards is also running, it handles the
// home cache too — only one of these needs to be started.
func (h *Handler) WarmHomeCache() {
	ctx := context.Background()
	log.Printf("home: warming leaderboard cache…")
	if _, err := h.buildHomeCache(ctx); err != nil {
		log.Printf("home: warm error: %v", err)
	} else {
		log.Printf("home: leaderboard cache ready")
	}

	// Refresh slightly before TTL expires so users never hit a cold cache.
	ticker := time.NewTicker(homeLBCacheTTL - 2*time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		log.Printf("home: refreshing leaderboard cache…")
		if _, err := h.buildHomeCache(ctx); err != nil {
			log.Printf("home: refresh error: %v", err)
		}
	}
}

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var lb homeLBCache
	if raw, ok := h.cache.Get(ctx, homeLBCacheKey); ok {
		_ = json.Unmarshal(raw, &lb)
	} else {
		// Cache miss — build synchronously (singleflight prevents stampede).
		lb, _ = h.buildHomeCache(ctx)
	}

	data := HomeData{
		TotalRepos:    lb.TotalRepos,
		TotalPRs:      lb.TotalPRs,
		TotalReviews:  lb.TotalReviews,
		SpeedDemons:   lb.SpeedDemons,
		PRGraveyard:   lb.PRGraveyard,
		ReviewChamps:  lb.ReviewChamps,
		Gatekeepers:   lb.Gatekeepers,
		MergeMasters:  lb.MergeMasters,
		OneShot:       lb.OneShot,
		PopularVisits: lb.PopularVisits,
		RecentVisits:  lb.RecentVisits,
	}
	data.OGDesc = fmt.Sprintf("%d PRs analyzed across %d repos. Global leaderboards for GitHub PR review time. If you aren't reviewing, you're ngmi.", data.TotalPRs, data.TotalRepos)
	data.BaseData = h.baseData(r)
	h.render(w, "home", data)
}

// LeaderboardAPI returns a leaderboard partial for HTMX category updates.
func (h *Handler) LeaderboardAPI(w http.ResponseWriter, r *http.Request) {
	category := r.URL.Query().Get("cat")

	type LeaderboardData struct {
		Category string
		Entries  []db.LeaderboardEntry
	}

	data := LeaderboardData{Category: category}

	switch category {
	case "speed":
		data.Entries, _ = h.db.LeaderboardReposBySpeed("ASC", 10)
	case "graveyard":
		data.Entries, _ = h.db.LeaderboardReposBySpeed("DESC", 10)
	case "reviewers":
		data.Entries, _ = h.db.LeaderboardReviewers(10)
	case "gatekeepers":
		data.Entries, _ = h.db.LeaderboardGatekeepers(10)
	case "authors":
		data.Entries, _ = h.db.LeaderboardAuthors(10)
	case "oneshot":
		data.Entries, _ = h.db.LeaderboardCleanApprovals(10)
	}

	h.renderPartial(w, "leaderboard", data)
}
