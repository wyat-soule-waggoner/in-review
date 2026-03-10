package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type RepoData struct {
	BaseData
	Repo         *db.Repo
	TopReviewers []db.ReviewerStats
	RecentPRs    []db.PullRequest
	SpeedRank    int
	IsSyncing    bool
	OwnerUser    *db.User
	Trim         int
	OGTitle      string
	OGDesc       string
	OGUrl        string
	ShareURL     string
}

const repoPageCacheTTL = 5 * time.Minute

// repoPageCache holds the non-chart DB-query results for a repo page.
type repoPageCache struct {
	TopReviewers []db.ReviewerStats `json:"topReviewers"`
	RecentPRs    []db.PullRequest   `json:"recentPRs"`
	SpeedRank    int                `json:"speedRank"`
}

// RepoChartsData is passed to the repo_charts partial.
type RepoChartsData struct {
	Owner         string
	Name          string
	Trim          int
	SizeChartJSON template.JS
	TimeChartJSON template.JS
}

const repoChartsCacheTTL = 5 * time.Minute

// repoChartsCache holds the chart query results for the lazy-loaded charts partial.
type repoChartsCache struct {
	SizeChartJSON string `json:"sizeChartJSON"`
	TimeChartJSON string `json:"timeChartJSON"`
}

// sizeChartPayload is marshaled to JSON and embedded directly in the repo page.
type sizeChartPayload struct {
	Labels       []string  `json:"labels"`
	PRCounts     []int     `json:"prCounts"`
	AvgHours     []float64 `json:"avgHours"`
	ApprovalRate []float64 `json:"approvalRate"`
}

func (h *Handler) Repo(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name

	trim, _ := parseTrim(r)

	// Ensure repo is in DB
	repo, _ := h.db.GetRepo(fullName)
	if repo == nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		ghRepo, err := h.gh.GetRepo(ctx, owner, name)
		if h.renderGHError(w, r, err, "Repo Not Found",
			"Could not find "+fullName+" on GitHub. Check the spelling and try again.") {
			return
		}
		// UpsertRepo may lose a race with the worker inserting the same repo;
		// ignore the error and re-read regardless.
		_ = h.db.UpsertRepo(db.Repo{
			FullName:    fullName,
			Owner:       owner,
			Name:        ghRepo.Name,
			Description: ghRepo.Description,
			Stars:       ghRepo.Stars,
			Language:    ghRepo.Language,
			SyncStatus:  "pending",
		})
		repo, _ = h.db.GetRepo(fullName)
		if repo == nil {
			// Build a minimal in-memory repo so the page still renders.
			repo = &db.Repo{
				FullName:    fullName,
				Owner:       owner,
				Name:        ghRepo.Name,
				Description: ghRepo.Description,
				Stars:       ghRepo.Stars,
				Language:    ghRepo.Language,
				SyncStatus:  "pending",
			}
		}
	}

	// Queue sync if needed
	h.worker.Queue(fullName, false)

	data := RepoData{
		Repo:      repo,
		IsSyncing: h.worker.IsSyncing(fullName),
		Trim:      trim,
	}
	data.OwnerUser, _ = h.db.GetUser(owner)

	// ── Cache check ──────────────────────────────────────────────────────────
	var rpc repoPageCache
	repoCacheKey := fmt.Sprintf("repo:v1:%s", fullName)
	repoCacheHit := false
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), repoCacheKey); ok {
			if json.Unmarshal(raw, &rpc) == nil {
				repoCacheHit = true
			}
		}
	}

	if !repoCacheHit {
		type reviewersRes struct{ v []db.ReviewerStats }
		type recentRes struct{ v []db.PullRequest }
		type rankRes struct{ v int }

		rvCh := make(chan reviewersRes, 1)
		rcCh := make(chan recentRes, 1)
		rkCh := make(chan rankRes, 1)

		go func() { v, _ := h.db.RepoTopReviewers(fullName, 10); rvCh <- reviewersRes{v} }()
		go func() { v, _ := h.db.RecentMergedPRs(fullName, 20); rcCh <- recentRes{v} }()
		go func() { v, _ := h.db.RepoSpeedRank(fullName); rkCh <- rankRes{v} }()

		rpc = repoPageCache{
			TopReviewers: (<-rvCh).v,
			RecentPRs:    (<-rcCh).v,
			SpeedRank:    (<-rkCh).v,
		}

		if h.cache != nil {
			if raw, err := json.Marshal(rpc); err == nil {
				h.cache.Set(r.Context(), repoCacheKey, raw, repoPageCacheTTL)
			}
		}
	}

	data.TopReviewers = rpc.TopReviewers
	data.RecentPRs = rpc.RecentPRs
	data.SpeedRank = rpc.SpeedRank

	// ── OG / share metadata ───────────────────────────────────────────────────
	data.OGTitle = fullName + " — ngmi"
	data.OGUrl = "https://ngmi.review/repo/" + fullName
	ogDesc := fullName
	if repo.AvgMergeTimeSecs > 0 {
		ogDesc += " merges PRs in " + formatDuration(repo.AvgMergeTimeSecs) + " on average"
	}
	if data.SpeedRank > 0 {
		ogDesc += fmt.Sprintf(" (#%d globally)", data.SpeedRank)
	}
	ogDesc += ". Track your repo at ngmi.review."
	data.OGDesc = ogDesc

	shareText := fullName
	if repo.AvgMergeTimeSecs > 0 {
		shareText += " merges PRs in " + formatDuration(repo.AvgMergeTimeSecs)
	}
	if data.SpeedRank > 0 {
		shareText += fmt.Sprintf(", #%d globally", data.SpeedRank)
	}
	shareText += ". If you aren't reviewing, you're ngmi."
	data.ShareURL = "https://twitter.com/intent/tweet?text=" + url.QueryEscape(shareText) +
		"&url=" + url.QueryEscape(data.OGUrl)

	data.BaseData = h.baseData(r)
	h.db.RecordVisit("/repo/"+fullName, "repo", fullName)
	h.render(w, "repo", data)
}

// RepoCharts returns the lazy-loaded charts partial for a repo page.
// It is called via HTMX (hx-trigger="load") so the heavy percentile queries
// don't block the initial page render.
func (h *Handler) RepoCharts(w http.ResponseWriter, r *http.Request) {
	owner    := chi.URLParam(r, "owner")
	name     := chi.URLParam(r, "name")
	fullName := owner + "/" + name
	trim, cutoffPct := parseTrim(r)

	cacheKey := fmt.Sprintf("repo:charts:v1:%s:%d", fullName, trim)
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			var cc repoChartsCache
			if json.Unmarshal(raw, &cc) == nil {
				h.renderPartial(w, "repo_charts", RepoChartsData{
					Owner:         owner,
					Name:          name,
					Trim:          trim,
					SizeChartJSON: template.JS(cc.SizeChartJSON),
					TimeChartJSON: template.JS(cc.TimeChartJSON),
				})
				return
			}
		}
	}

	type bucketsRes struct{ v []db.PRSizeBucket }
	type pointsRes struct{ v []db.TimeSeriesPoint }
	buCh := make(chan bucketsRes, 1)
	ptCh := make(chan pointsRes, 1)
	go func() { v, _ := h.db.RepoSizeChartData(fullName, cutoffPct); buCh <- bucketsRes{v} }()
	go func() { v, _ := h.db.RepoTimeSeriesData(fullName, cutoffPct); ptCh <- pointsRes{v} }()

	buckets := (<-buCh).v
	points  := (<-ptCh).v

	cd := RepoChartsData{Owner: owner, Name: name, Trim: trim}

	if len(buckets) > 0 {
		payload := sizeChartPayload{}
		for _, b := range buckets {
			payload.Labels = append(payload.Labels, b.Label)
			payload.PRCounts = append(payload.PRCounts, b.PRCount)
			payload.AvgHours = append(payload.AvgHours, roundTo1(b.AvgSecs/3600))
			payload.ApprovalRate = append(payload.ApprovalRate, roundTo1(b.ApprovalRate))
		}
		if raw, err := json.Marshal(payload); err == nil {
			cd.SizeChartJSON = template.JS(raw)
		}
	}

	if len(points) > 0 {
		tp := timeChartPayload{}
		for _, p := range points {
			tp.Labels = append(tp.Labels, p.Label)
			tp.PRCounts = append(tp.PRCounts, p.PRCount)
			tp.AvgSize = append(tp.AvgSize, roundTo1(p.AvgSize))
			tp.MedianSize = append(tp.MedianSize, roundTo1(p.MedianSize))
			tp.AvgHours = append(tp.AvgHours, roundTo1(p.AvgSecs/3600))
			tp.MedianHours = append(tp.MedianHours, roundTo1(p.MedianSecs/3600))
			tp.ChangesRequestedRate = append(tp.ChangesRequestedRate, roundTo1(p.ChangesRequestedRate))
			tp.AvgFirstReviewHours = append(tp.AvgFirstReviewHours, roundTo1(p.AvgFirstReviewSecs/3600))
			tp.MedFirstReviewHours = append(tp.MedFirstReviewHours, roundTo1(p.MedFirstReviewSecs/3600))
			tp.UnreviewedMergeRate = append(tp.UnreviewedMergeRate, roundTo1(p.UnreviewedRate))
			tp.LinesPerContrib = append(tp.LinesPerContrib, roundTo1(p.LinesPerContrib))
		}
		if raw, err := json.Marshal(tp); err == nil {
			cd.TimeChartJSON = template.JS(raw)
		}
	}

	if h.cache != nil {
		cc := repoChartsCache{
			SizeChartJSON: string(cd.SizeChartJSON),
			TimeChartJSON: string(cd.TimeChartJSON),
		}
		if raw, err := json.Marshal(cc); err == nil {
			h.cache.Set(r.Context(), cacheKey, raw, repoChartsCacheTTL)
		}
	}

	h.renderPartial(w, "repo_charts", cd)
}

// TriggerSync forces a fresh sync for a repo.
func (h *Handler) TriggerSync(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name
	h.worker.Queue(fullName, true)
	w.Header().Set("HX-Refresh", "true")
	w.WriteHeader(http.StatusNoContent)
}

// SyncStatus returns a small HTML snippet with current sync state (polled by HTMX).
func (h *Handler) SyncStatus(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name

	qpos := h.worker.QueuePosition(fullName)
	repo, _ := h.db.GetRepo(fullName)

	poll := `hx-get="/api/sync-status/` + owner + `/` + name + `" hx-trigger="every 2s" hx-swap="outerHTML"`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	switch {
	case qpos > 0:
		// Waiting in queue — show position
		w.Write([]byte(`<span class="sync-badge syncing" ` + poll + `>⟳ Queue position #` + fmt.Sprintf("%d", qpos) + `</span>`))
	case qpos == 0:
		// Popped from queue, worker is actively fetching
		w.Write([]byte(`<span class="sync-badge syncing" ` + poll + `>⟳ Syncing…</span>`))
	case repo != nil && repo.LastSynced != nil:
		w.Write([]byte(`<span class="sync-badge done">✓ Synced ` + timeAgo(repo.LastSynced) + `</span>`))
	default:
		w.Write([]byte(`<span class="sync-badge pending">⏳ Pending</span>`))
	}
}
