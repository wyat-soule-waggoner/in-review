package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"inreview/internal/auth"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
)

const (
	maxPRsPerRepo = 5000
	syncCooldown  = 6 * time.Hour
	workerCount   = 10
	popTimeout    = 30 * time.Second
)

type cachedToken struct {
	token     string
	expiresAt time.Time
}

// Worker manages background GitHub sync jobs.
type Worker struct {
	gh         *github.Client
	db         *db.DB
	rdb        *rdb.Client
	appID      int64
	appPrivKey string

	tokenMu    sync.RWMutex
	tokenCache map[int64]cachedToken // installation ID → token
}

func New(gh *github.Client, db *db.DB, rdb *rdb.Client, appID int64, appPrivKey string) *Worker {
	return &Worker{
		gh:         gh,
		db:         db,
		rdb:        rdb,
		appID:      appID,
		appPrivKey: appPrivKey,
		tokenCache: make(map[int64]cachedToken),
	}
}

// installationClient returns a GitHub client using the installation token for
// the given org/owner login. Falls back to the global client if the app is not
// configured or no installation is found.
func (w *Worker) installationClient(ctx context.Context, ownerLogin string) *github.Client {
	if w.appID == 0 || w.appPrivKey == "" {
		return w.gh
	}

	instID, err := w.db.GetInstallationByLogin(ownerLogin)
	if err != nil || instID == nil {
		return w.gh
	}

	// Check cache under read lock — hot path, no blocking.
	w.tokenMu.RLock()
	entry, ok := w.tokenCache[*instID]
	w.tokenMu.RUnlock()
	if ok && time.Now().Before(entry.expiresAt) {
		return w.gh.WithToken(entry.token)
	}

	// Cache miss: generate a fresh token WITHOUT holding the lock.
	// Two goroutines may race here on first use, but both tokens are valid
	// and only one extra API call is wasted — far better than blocking all workers.
	key, err := auth.ParsePrivateKey(w.appPrivKey)
	if err != nil {
		log.Printf("[worker] parse app private key: %v", err)
		return w.gh
	}
	appJWT, err := auth.GenerateAppJWT(w.appID, key)
	if err != nil {
		log.Printf("[worker] generate app JWT: %v", err)
		return w.gh
	}
	token, expiresAt, err := auth.GetInstallationToken(appJWT, fmt.Sprintf("%d", *instID))
	if err != nil {
		log.Printf("[worker] get installation token for %s: %v", ownerLogin, err)
		return w.gh
	}

	w.tokenMu.Lock()
	w.tokenCache[*instID] = cachedToken{token: token, expiresAt: expiresAt.Add(-2 * time.Minute)}
	w.tokenMu.Unlock()

	log.Printf("[worker] fetched installation token for %s (inst %d, expires %s)", ownerLogin, *instID, expiresAt.Format(time.RFC3339))
	return w.gh.WithToken(token)
}

// Start launches background sync goroutines and the pending-repo sweeper.
func (w *Worker) Start() {
	for i := 0; i < workerCount; i++ {
		go w.run()
	}
	go w.sweepLoop()
}

// sweepLoop periodically picks up repos stuck in 'pending' status and queues them.
// This handles the case where pending repos lost their Redis queue entry after a restart.
const (
	sweepInterval  = 5 * time.Minute
	sweepBatch     = 50            // repos to queue per sweep tick
	errorCooldown  = 1 * time.Hour // minimum wait before retrying an errored repo
)

func (w *Worker) sweepLoop() {
	// Initial sweep after a short delay to let workers warm up.
	time.Sleep(30 * time.Second)
	w.sweep()

	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		w.sweep()
	}
}

func (w *Worker) sweep() {
	repos, err := w.db.UnsyncedRepos(sweepBatch, errorCooldown)
	if err != nil {
		log.Printf("[sweep] db error: %v", err)
		return
	}
	if len(repos) == 0 {
		return
	}
	queued := 0
	for _, fullName := range repos {
		w.Queue(fullName, false)
		queued++
	}
	log.Printf("[sweep] queued %d repos (pending or retryable errors)", queued)
}

func (w *Worker) run() {
	ctx := context.Background()
	for {
		fullName, ok := w.rdb.QPop(ctx, popTimeout)
		if !ok {
			continue
		}
		w.syncRepo(fullName)
	}
}

// Queue schedules a repo sync unless one is already in-flight or recently completed.
// Set force=true to bypass the cooldown check.
func (w *Worker) Queue(fullName string, force bool) {
	ctx := context.Background()
	if w.rdb.QIsInProgress(ctx, fullName) {
		return
	}
	if !force {
		repo, _ := w.db.GetRepo(fullName)
		if repo != nil && repo.LastSynced != nil &&
			time.Since(*repo.LastSynced) < syncCooldown &&
			repo.SyncStatus == "done" {
			return
		}
	}
	// Reserve the slot before pushing so concurrent Queue calls are idempotent.
	w.rdb.QMarkInProgress(ctx, fullName)
	if err := w.rdb.QPush(ctx, fullName); err != nil {
		log.Printf("[queue] failed to push %s: %v", fullName, err)
		w.rdb.QMarkDone(ctx, fullName)
	}
}

// IsSyncing returns true if the repo is currently queued or being synced.
func (w *Worker) IsSyncing(fullName string) bool {
	return w.rdb.QIsInProgress(context.Background(), fullName)
}

// QueuePosition returns the 1-based position in the pending queue, or 0 if the
// repo is being actively synced by a worker, or -1 if not in progress at all.
func (w *Worker) QueuePosition(fullName string) int {
	ctx := context.Background()
	if !w.rdb.QIsInProgress(ctx, fullName) {
		return -1
	}
	pos := w.rdb.QPosition(ctx, fullName)
	if pos < 0 {
		return 0 // in-progress key set but not in queue → actively syncing
	}
	return int(pos) + 1 // convert to 1-based
}

// syncRepo performs the full GitHub data pull for one repository.
func (w *Worker) syncRepo(fullName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	defer w.rdb.QMarkDone(ctx, fullName)

	parts := strings.SplitN(fullName, "/", 2)
	if len(parts) != 2 {
		return
	}
	owner, name := parts[0], parts[1]
	log.Printf("[sync] starting %s", fullName)
	w.db.UpdateSyncStatus(fullName, "syncing")

	gh := w.installationClient(ctx, owner)
	result, err := gh.SyncRepo(ctx, owner, name, maxPRsPerRepo)
	if err != nil {
		if errors.Is(err, github.ErrNotFound) {
			log.Printf("[sync] abandoned %s: repo not found (deleted or private)", fullName)
			w.db.UpdateSyncStatus(fullName, "abandoned")
		} else {
			log.Printf("[sync] error fetching %s: %v", fullName, err)
			w.db.UpdateSyncStatus(fullName, "error")
		}
		return
	}

	// ── Owner / org metadata ──────────────────────────────────────────────────
	isOrg := result.Owner.Type == "Organization"
	orgName := ""
	if isOrg {
		orgName = owner
	}
	w.db.UpsertUser(db.User{
		Login:       result.Owner.Login,
		Name:        result.Owner.Name,
		AvatarURL:   result.Owner.AvatarURL,
		Bio:         result.Owner.Bio,
		PublicRepos: result.Owner.PublicRepos,
		Followers:   result.Owner.Followers,
		Company:     result.Owner.Company,
		Location:    result.Owner.Location,
		IsOrg:       isOrg,
	})

	// ── Repo metadata ─────────────────────────────────────────────────────────
	w.db.UpsertRepo(db.Repo{
		FullName:    fullName,
		Owner:       owner,
		Name:        name,
		Description: result.Repo.Description,
		Stars:       result.Repo.Stars,
		Language:    result.Repo.Language,
		OrgName:     orgName,
		SyncStatus:  "syncing",
	})

	log.Printf("[sync] %s: %d merged PRs", fullName, len(result.PRs))

	// ── Pull requests + reviews ───────────────────────────────────────────────
	for _, ghPR := range result.PRs {
		w.db.UpsertUser(db.User{Login: ghPR.Author})

		var mts *int64
		if ghPR.MergedAt != nil {
			secs := int64(ghPR.MergedAt.Sub(ghPR.CreatedAt).Seconds())
			mts = &secs
		}

		pr := db.PullRequest{
			ID:            fmt.Sprintf("%s#%d", fullName, ghPR.Number),
			RepoFullName:  fullName,
			Number:        ghPR.Number,
			Title:         ghPR.Title,
			AuthorLogin:   ghPR.Author,
			Merged:        ghPR.MergedAt != nil,
			OpenedAt:      ghPR.CreatedAt,
			MergedAt:      ghPR.MergedAt,
			MergeTimeSecs: mts,
			ReviewCount:   len(ghPR.Reviews),
			Additions:     ghPR.Additions,
			Deletions:     ghPR.Deletions,
		}

		for _, rev := range ghPR.Reviews {
			if rev.State == "CHANGES_REQUESTED" {
				pr.ChangesRequestedCount++
			}
			if pr.FirstReviewAt == nil || rev.SubmittedAt.Before(*pr.FirstReviewAt) {
				t := rev.SubmittedAt
				pr.FirstReviewAt = &t
			}
			w.db.UpsertUser(db.User{
				Login:     rev.User.Login,
				AvatarURL: rev.User.AvatarURL,
			})
			w.db.UpsertReview(db.Review{
				ID:            fmt.Sprintf("%d", rev.ID),
				RepoFullName:  fullName,
				PRNumber:      ghPR.Number,
				ReviewerLogin: rev.User.Login,
				State:         rev.State,
				SubmittedAt:   rev.SubmittedAt,
			})
		}

		w.db.UpsertPR(pr)
	}

	w.db.UpdateRepoStats(fullName)
	w.db.UpdateSyncStatus(fullName, "done")
	log.Printf("[sync] done %s", fullName)
}
