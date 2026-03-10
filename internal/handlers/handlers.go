package handlers

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"inreview/internal/analytics"
	"inreview/internal/config"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
)

// contextKey is a private type for context values to avoid collisions.
type contextKey string

const (
	userLoginKey      contextKey = "userLogin"
	installationIDKey contextKey = "installationID"
)

// BaseData is embedded in every full-page data struct so the layout template
// can access the currently logged-in user without extra queries per handler.
type BaseData struct {
	CurrentUser string
}

// baseData builds a BaseData from the current request context.
func (h *Handler) baseData(r *http.Request) BaseData {
	return BaseData{CurrentUser: currentUser(r)}
}

// currentUser extracts the GitHub login from the request context.
// Returns "" when not authenticated.
func currentUser(r *http.Request) string {
	login, _ := r.Context().Value(userLoginKey).(string)
	return login
}

// installationID extracts the installation ID from the request context.
// Returns 0 when not present.
func installationID(r *http.Request) int64 {
	id, _ := r.Context().Value(installationIDKey).(int64)
	return id
}

// SessionLoader is a global middleware that loads session info from the
// session_id cookie and injects the GitHub login + installation ID into the
// request context. Public routes benefit from this too (e.g. nav state).
func (h *Handler) SessionLoader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cookie, err := r.Cookie("session_id"); err == nil {
			if session, err := h.db.GetSession(cookie.Value); err == nil && session != nil {
				ctx := context.WithValue(r.Context(), userLoginKey, session.Login)
				if session.InstallationID != nil {
					ctx = context.WithValue(ctx, installationIDKey, *session.InstallationID)
				}
				r = r.WithContext(ctx)
			}
		}
		next.ServeHTTP(w, r)
	})
}

// TrackPageViews is a middleware that captures PostHog page_viewed events for
// full-page GET requests. HTMX partial requests (HX-Request header present)
// are skipped so only meaningful navigations are tracked.
func (h *Handler) TrackPageViews(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if r.Method != http.MethodGet || r.Header.Get("HX-Request") != "" {
			return
		}
		user := currentUser(r)
		distinctID := user
		if distinctID == "" {
			distinctID = "anonymous"
		}
		h.analytics.Capture(distinctID, "page_viewed", map[string]interface{}{
			"path":         r.URL.Path,
			"$current_url": r.URL.String(),
		})
	})
}

// RequireAuth wraps a handler and redirects unauthenticated users to /auth/github.
func (h *Handler) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if currentUser(r) == "" {
			http.Redirect(w, r, "/auth/github", http.StatusFound)
			return
		}
		next(w, r)
	}
}

// Queuer is the subset of worker.Worker used by HTTP handlers.
// Both the web-only and combined binaries satisfy this with a *worker.Worker;
// the difference is whether Worker.Start() has been called.
type Queuer interface {
	Queue(fullName string, force bool)
	IsSyncing(fullName string) bool
	QueuePosition(fullName string) int
}

// Handler holds all dependencies and parsed templates.
type Handler struct {
	db        *db.DB
	gh        *github.Client
	worker    Queuer
	cache     *rdb.Client
	cfg       *config.Config
	analytics *analytics.Client
	tmpls     map[string]*template.Template
	funcMap   template.FuncMap
}

func New(database *db.DB, gh *github.Client, w Queuer, cache *rdb.Client, cfg *config.Config, a *analytics.Client) *Handler {
	h := &Handler{
		db:        database,
		gh:        gh,
		worker:    w,
		cache:     cache,
		cfg:       cfg,
		analytics: a,
	}
	h.funcMap = template.FuncMap{
		"formatDuration":      formatDuration,
		"formatDurationShort": formatDurationShort,
		"timeAgo":             timeAgo,
		"formatNumber":        formatNumber,
		"rankBadge":           rankBadge,
		"rankClass":           rankClass,
		"percent": func(a, b int) int {
			if b == 0 {
				return 0
			}
			return (a * 100) / b
		},
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"deref64": func(p *int64) int64 {
			if p == nil {
				return 0
			}
			return *p
		},
		"int64ToInt":  func(v int64) int { return int(v) },
		"floatToInt":  func(v float64) int { return int(v) },
		"floatToInt64": func(v float64) int64 { return int64(v) },
	}
	h.loadTemplates()
	return h
}

func (h *Handler) loadTemplates() {
	h.tmpls = make(map[string]*template.Template)

	pages := []string{"home", "repo", "user", "org", "leaderboard_page", "error", "hi_wall", "stats", "dashboard"}
	for _, page := range pages {
		tmpl := template.Must(
			template.New("").Funcs(h.funcMap).ParseFiles(
				filepath.Join("templates", "layout.html"),
				filepath.Join("templates", page+".html"),
			),
		)
		h.tmpls[page] = tmpl
	}

	// blog page needs blog_stats partial so {{template "blog_stats" .}} works inline.
	h.tmpls["blog"] = template.Must(
		template.New("").Funcs(h.funcMap).ParseFiles(
			filepath.Join("templates", "layout.html"),
			filepath.Join("templates", "blog.html"),
			filepath.Join("templates", "partials", "blog_stats.html"),
		),
	)

	// data page needs all data partials so {{template "data_repos" .}} works inline.
	h.tmpls["data"] = template.Must(
		template.New("").Funcs(h.funcMap).ParseFiles(
			filepath.Join("templates", "layout.html"),
			filepath.Join("templates", "data.html"),
			filepath.Join("templates", "partials", "data_repos.html"),
			filepath.Join("templates", "partials", "data_prs.html"),
			filepath.Join("templates", "partials", "data_reviews.html"),
			filepath.Join("templates", "partials", "data_users.html"),
		),
	)

	partials := []string{
		"search_results", "leaderboard", "leaderboard_rows", "leaderboard_search",
		"data_repos", "data_prs", "data_reviews", "data_users",
		"blog_stats",
	}
	for _, partial := range partials {
		tmpl := template.Must(
			template.New(partial).Funcs(h.funcMap).ParseFiles(
				filepath.Join("templates", "partials", partial+".html"),
			),
		)
		h.tmpls[partial] = tmpl
	}
}

// ErrorData is passed to the error template.
type ErrorData struct {
	BaseData
	Code     int
	Title    string
	Message  string
	Detail   string
	RetryURL string
	OGTitle  string
	OGDesc   string
	OGUrl    string
}

// renderError renders the error page with the given HTTP status code.
func (h *Handler) renderError(w http.ResponseWriter, code int, title, message string) {
	h.renderErrorReq(w, nil, code, title, message)
}

// renderErrorReq renders the error page with session context (for nav state).
func (h *Handler) renderErrorReq(w http.ResponseWriter, r *http.Request, code int, title, message string) {
	tmpl, ok := h.tmpls["error"]
	if !ok {
		http.Error(w, message, code)
		return
	}
	var base BaseData
	if r != nil {
		base = h.baseData(r)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	if err := tmpl.ExecuteTemplate(w, "layout", ErrorData{
		BaseData: base,
		Code:     code,
		Title:    title,
		Message:  message,
	}); err != nil {
		log.Printf("error template error: %v", err)
	}
}

// renderGHError translates a GitHub client error into an appropriate page.
// Returns true if it rendered a response (caller should return immediately).
// notFoundTitle/Msg are shown only for ErrNotFound; rate-limit and other
// errors get their own messages regardless of what the caller passes.
func (h *Handler) renderGHError(w http.ResponseWriter, r *http.Request, err error, notFoundTitle, notFoundMsg string) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, github.ErrRateLimited):
		h.renderErrorReq(w, r, http.StatusTooManyRequests,
			"GitHub Rate Limit Reached",
			"The GitHub API hourly rate limit has been hit. Cached data is still available on pages already synced. Try again in a few minutes.")
	case errors.Is(err, github.ErrNotFound):
		h.renderErrorReq(w, r, http.StatusNotFound, notFoundTitle, notFoundMsg)
	default:
		log.Printf("github error: %v", err)
		h.renderErrorReq(w, r, http.StatusBadGateway,
			"GitHub Unavailable",
			"Couldn't reach GitHub right now. Try again in a moment.")
	}
	return true
}

// render executes the full layout template for a page.
func (h *Handler) render(w http.ResponseWriter, name string, data interface{}) {
	tmpl, ok := h.tmpls[name]
	if !ok {
		http.Error(w, fmt.Sprintf("template %q not found", name), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "layout", data); err != nil {
		log.Printf("template %q error: %v", name, err)
	}
}

// renderPartial executes a named partial template (for HTMX responses).
func (h *Handler) renderPartial(w http.ResponseWriter, name string, data interface{}) {
	tmpl, ok := h.tmpls[name]
	if !ok {
		http.Error(w, fmt.Sprintf("partial %q not found", name), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("partial %q error: %v", name, err)
	}
}

// ── Template helpers ───────────────────────────────────────────────────────────

func formatDuration(secs int64) string {
	if secs <= 0 {
		return "—"
	}
	d := time.Duration(secs) * time.Second
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day"
	}
	if days < 30 {
		return fmt.Sprintf("%d days", days)
	}
	months := days / 30
	if months == 1 {
		return "1 month"
	}
	if months < 12 {
		return fmt.Sprintf("%d months", months)
	}
	years := days / 365
	if years == 1 {
		return "1 year"
	}
	return fmt.Sprintf("%d years", years)
}

func formatDurationShort(secs int64) string {
	if secs <= 0 {
		return "—"
	}
	d := time.Duration(secs) * time.Second
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func timeAgo(t *time.Time) string {
	if t == nil {
		return "never"
	}
	d := time.Since(*t)
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day ago"
	}
	if days < 30 {
		return fmt.Sprintf("%d days ago", days)
	}
	months := days / 30
	if months == 1 {
		return "1 month ago"
	}
	return fmt.Sprintf("%d months ago", months)
}

func formatNumber(n int) string {
	if n < 1_000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
}

func rankBadge(rank int) string {
	switch rank {
	case 1:
		return "#1"
	case 2:
		return "#2"
	case 3:
		return "#3"
	default:
		return fmt.Sprintf("#%d", rank)
	}
}

func roundTo1(f float64) float64 {
	return float64(int(f*10+0.5)) / 10
}

func rankClass(rank int) string {
	switch rank {
	case 1:
		return "rank-gold"
	case 2:
		return "rank-silver"
	case 3:
		return "rank-bronze"
	default:
		return "rank-other"
	}
}
