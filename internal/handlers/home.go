package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

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
const homeLBCacheTTL = 3 * time.Minute

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var lb homeLBCache
	if raw, ok := h.cache.Get(ctx, homeLBCacheKey); ok {
		_ = json.Unmarshal(raw, &lb)
	} else {
		// Run all independent queries concurrently; latency = slowest query.
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
