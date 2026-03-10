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

const userPageCacheTTL = 5 * time.Minute

// userPageCache holds the DB-query results for a user page, serialized to Redis.
// Chart data (activity/size) is excluded — it lives in its own cache via UserCharts.
type userPageCache struct {
	ReviewerStats  *db.ReviewerStats   `json:"reviewerStats"`
	AuthorStats    *db.AuthorStats     `json:"authorStats"`
	ReviewerRank   int                 `json:"reviewerRank"`
	GatekeeperRank int                 `json:"gatekeeperRank"`
	AuthorRank     int                 `json:"authorRank"`
	ContribRepos   []db.Repo           `json:"contribRepos"`
	FastestPR      *db.UserRecordPR    `json:"fastestPR"`
	SlowestPR      *db.UserRecordPR    `json:"slowestPR"`
	ReviewedRepos  []db.UserRepoReview `json:"reviewedRepos"`
	ReviewersOfMe  []db.CollabEntry    `json:"reviewersOfMe"`
	AuthorsIReview []db.CollabEntry    `json:"authorsIReview"`
}

// UserChartsData is passed to the user_charts partial.
type UserChartsData struct {
	ActivityJSON   template.JS
	SizeBucketJSON template.JS
}

const userChartsCacheTTL = 5 * time.Minute

type userChartsCache struct {
	ActivityJSON   string `json:"activityJSON"`
	SizeBucketJSON string `json:"sizeBucketJSON"`
}

type userActivityPayload struct {
	Labels      []string  `json:"labels"`
	PRCounts    []int     `json:"prCounts"`
	ReviewCounts []int    `json:"reviewCounts"`
	CRRate      []float64 `json:"crRate"`
}

type userSizePayload struct {
	Labels   []string `json:"labels"`
	PRCounts []int    `json:"prCounts"`
}

type UserData struct {
	BaseData
	User             *db.User
	ReviewerStats    *db.ReviewerStats
	AuthorStats      *db.AuthorStats
	ReviewerRank     int
	GatekeeperRank   int
	AuthorRank       int
	ContributedRepos []db.Repo
	FastestPR        *db.UserRecordPR
	SlowestPR        *db.UserRecordPR
	ReviewedRepos    []db.UserRepoReview
	ReviewersOfMe    []db.CollabEntry
	AuthorsIReview   []db.CollabEntry
	IsOrg            bool
	IsNGMI           bool
	OGTitle          string
	OGDesc           string
	OGUrl            string
	ShareURL         string
}

func (h *Handler) User(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")

	// Fetch from GitHub to get latest info
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	ghUser, err := h.gh.GetUser(ctx, username)
	if err != nil {
		// If we have the user cached in the DB, render from that instead of erroring.
		if cached, dbErr := h.db.GetUser(username); dbErr == nil && cached != nil {
			// Serve cached page — fall through with ghUser = nil
		} else {
			h.renderGHError(w, r, err, "User Not Found",
				"Could not find @"+username+" on GitHub. Check the spelling and try again.")
			return
		}
	}

	// Determine if org and redirect early (only when we have fresh GitHub data).
	if ghUser != nil && ghUser.Type == "Organization" {
		http.Redirect(w, r, "/org/"+username, http.StatusFound)
		return
	}

	// Cache user if we got fresh data.
	if ghUser != nil {
		h.db.UpsertUser(db.User{
			Login:       ghUser.Login,
			Name:        ghUser.Name,
			AvatarURL:   ghUser.AvatarURL,
			Bio:         ghUser.Bio,
			PublicRepos: ghUser.PublicRepos,
			Followers:   ghUser.Followers,
			Company:     ghUser.Company,
			Location:    ghUser.Location,
			IsOrg:       false,
		})
	}

	user, _ := h.db.GetUser(username)
	if user == nil && ghUser != nil {
		user = &db.User{
			Login:     ghUser.Login,
			Name:      ghUser.Name,
			AvatarURL: ghUser.AvatarURL,
		}
	}
	if user == nil {
		h.renderGHError(w, r, err, "User Not Found",
			"Could not find @"+username+" on GitHub. Check the spelling and try again.")
		return
	}

	// Queue top owned repos + repos where they've reviewed PRs for sync.
	go func() {
		bg := context.Background()
		if repos, err := h.gh.GetUserRepos(bg, username, 10); err == nil {
			for _, repo := range repos {
				h.db.UpsertRepo(db.Repo{
					FullName:    repo.FullName,
					Owner:       repo.Owner.Login,
					Name:        repo.Name,
					Description: repo.Description,
					Stars:       repo.Stars,
					Language:    repo.Language,
					SyncStatus:  "pending",
				})
				h.worker.Queue(repo.FullName, false)
			}
		}
		if reviewedRepos, err := h.gh.GetReviewedRepos(bg, username, 100); err == nil {
			for _, fullName := range reviewedRepos {
				h.worker.Queue(fullName, false)
			}
		}
	}()

	// ── Cache check ──────────────────────────────────────────────────────────
	var pc userPageCache
	cacheKey := fmt.Sprintf("user:v1:%s", username)
	cacheHit := false
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			if json.Unmarshal(raw, &pc) == nil {
				cacheHit = true
			}
		}
	}

	if !cacheHit {
		type rrRes struct {
			v   *db.ReviewerStats
			err error
		}
		type arRes struct {
			v   *db.AuthorStats
			err error
		}
		type recRes struct {
			fastest, slowest *db.UserRecordPR
		}
		type collabRes struct {
			reviewersOfMe  []db.CollabEntry
			authorsIReview []db.CollabEntry
		}
		type reviewedReposRes struct {
			v   []db.UserRepoReview
			err error
		}

		rrCh    := make(chan rrRes, 1)
		arCh    := make(chan arRes, 1)
		rrankCh := make(chan int, 1)
		gkCh    := make(chan int, 1)
		auCh    := make(chan int, 1)
		contCh  := make(chan []db.Repo, 1)
		recCh   := make(chan recRes, 1)
		colCh   := make(chan collabRes, 1)
		rvRepCh := make(chan reviewedReposRes, 1)

		go func() { v, err := h.db.UserReviewerStats(username); rrCh <- rrRes{v, err} }()
		go func() { v, err := h.db.UserAuthorStats(username); arCh <- arRes{v, err} }()
		go func() { v, _ := h.db.UserReviewerRank(username); rrankCh <- v }()
		go func() { v, _ := h.db.UserGatekeeperRank(username); gkCh <- v }()
		go func() { v, _ := h.db.UserAuthorRank(username); auCh <- v }()
		go func() { v, _ := h.db.UserContributedRepos(username, 10); contCh <- v }()
		go func() {
			f, s, _ := h.db.UserRecordPRs(username)
			recCh <- recRes{f, s}
		}()
		go func() {
			rm, ai, _ := h.db.UserTopCollaborators(username, 5)
			colCh <- collabRes{rm, ai}
		}()
		go func() { v, err := h.db.UserTopReviewedRepos(username, 8); rvRepCh <- reviewedReposRes{v, err} }()

		rrResult := <-rrCh
		arResult := <-arCh
		rec      := <-recCh
		col      := <-colCh
		rvRep    := <-rvRepCh

		pc = userPageCache{
			ReviewerStats:  rrResult.v,
			AuthorStats:    arResult.v,
			ReviewerRank:   <-rrankCh,
			GatekeeperRank: <-gkCh,
			AuthorRank:     <-auCh,
			ContribRepos:   <-contCh,
			FastestPR:      rec.fastest,
			SlowestPR:      rec.slowest,
			ReviewedRepos:  rvRep.v,
			ReviewersOfMe:  col.reviewersOfMe,
			AuthorsIReview: col.authorsIReview,
		}

		if h.cache != nil {
			if raw, err := json.Marshal(pc); err == nil {
				h.cache.Set(r.Context(), cacheKey, raw, userPageCacheTTL)
			}
		}
	}

	data := UserData{
		User:             user,
		IsOrg:            false,
		ReviewerStats:    pc.ReviewerStats,
		AuthorStats:      pc.AuthorStats,
		ReviewerRank:     pc.ReviewerRank,
		GatekeeperRank:   pc.GatekeeperRank,
		AuthorRank:       pc.AuthorRank,
		ContributedRepos: pc.ContribRepos,
		FastestPR:        pc.FastestPR,
		SlowestPR:        pc.SlowestPR,
		ReviewersOfMe:    pc.ReviewersOfMe,
		AuthorsIReview:   pc.AuthorsIReview,
		ReviewedRepos:    pc.ReviewedRepos,
	}

	data.IsNGMI = data.ReviewerStats == nil || data.ReviewerStats.TotalReviews < 10

	// ── OG / share metadata ───────────────────────────────────────────────────
	data.OGUrl = "https://ngmi.review/user/" + username
	displayName := username
	if user.Name != "" {
		displayName = user.Name
	}
	data.OGTitle = "@" + username + " — ngmi"
	if data.ReviewerStats != nil && data.ReviewerStats.TotalReviews > 0 {
		approvalPct := (data.ReviewerStats.Approvals * 100) / data.ReviewerStats.TotalReviews
		ogDesc := fmt.Sprintf("%s: %d reviews, %d%% approval rate", displayName, data.ReviewerStats.TotalReviews, approvalPct)
		if data.ReviewerRank > 0 {
			ogDesc += fmt.Sprintf(" (#%d globally)", data.ReviewerRank)
		}
		ogDesc += ". If you aren't reviewing, you're ngmi."
		data.OGDesc = ogDesc

		var shareText string
		if data.ReviewerRank > 0 {
			shareText = fmt.Sprintf("#%d code reviewer globally — %d reviews, %d%% clean approvals. If you aren't reviewing, you're ngmi.", data.ReviewerRank, data.ReviewerStats.TotalReviews, approvalPct)
		} else {
			shareText = fmt.Sprintf("%d code reviews, %d%% approval rate. If you aren't reviewing, you're ngmi.", data.ReviewerStats.TotalReviews, approvalPct)
		}
		data.ShareURL = "https://twitter.com/intent/tweet?text=" + url.QueryEscape(shareText) +
			"&url=" + url.QueryEscape(data.OGUrl)
	} else {
		data.OGDesc = "@" + username + " has no reviews on record. ngmi."
	}

	data.BaseData = h.baseData(r)
	h.db.RecordVisit("/user/"+username, "user", username)
	h.render(w, "user", data)
}

// UserCharts returns the lazy-loaded charts partial for a user page.
// Called via HTMX (hx-trigger="load") so chart queries don't block the initial render.
func (h *Handler) UserCharts(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")

	cacheKey := fmt.Sprintf("user:charts:v1:%s", username)
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			var cc userChartsCache
			if json.Unmarshal(raw, &cc) == nil {
				h.renderPartial(w, "user_charts", UserChartsData{
					ActivityJSON:   template.JS(cc.ActivityJSON),
					SizeBucketJSON: template.JS(cc.SizeBucketJSON),
				})
				return
			}
		}
	}

	type actRes struct{ v []db.UserActivityPoint }
	type sizeRes struct{ v []db.PRSizeBucket }
	actCh  := make(chan actRes, 1)
	sizeCh := make(chan sizeRes, 1)
	go func() { v, _ := h.db.UserActivitySeries(username); actCh <- actRes{v} }()
	go func() { v, _ := h.db.UserPRSizeDist(username); sizeCh <- sizeRes{v} }()

	activity := (<-actCh).v
	sizeDist := (<-sizeCh).v

	cd := UserChartsData{}

	if len(activity) > 0 {
		ap := userActivityPayload{}
		for _, p := range activity {
			ap.Labels = append(ap.Labels, p.Label)
			ap.PRCounts = append(ap.PRCounts, p.PRCount)
			ap.ReviewCounts = append(ap.ReviewCounts, p.ReviewCount)
			ap.CRRate = append(ap.CRRate, roundTo1(p.ChangesRequestedRate))
		}
		if raw, err := json.Marshal(ap); err == nil {
			cd.ActivityJSON = template.JS(raw)
		}
	}

	if len(sizeDist) > 0 {
		sp := userSizePayload{}
		for _, b := range sizeDist {
			sp.Labels = append(sp.Labels, b.Label)
			sp.PRCounts = append(sp.PRCounts, b.PRCount)
		}
		if raw, err := json.Marshal(sp); err == nil {
			cd.SizeBucketJSON = template.JS(raw)
		}
	}

	if h.cache != nil {
		cc := userChartsCache{
			ActivityJSON:   string(cd.ActivityJSON),
			SizeBucketJSON: string(cd.SizeBucketJSON),
		}
		if raw, err := json.Marshal(cc); err == nil {
			h.cache.Set(r.Context(), cacheKey, raw, userChartsCacheTTL)
		}
	}

	h.renderPartial(w, "user_charts", cd)
}
