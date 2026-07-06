package dbsites

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(new(Handler))
}

const defaultCacheClearPath = "/db-sites/cache/clear"
const routeQueryVersion = "custom-domain-route-v3-explicit-status-aliases"

var safeIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

type Handler struct {
	DatabaseURL string `json:"database_url,omitempty"`
	Schema      string `json:"schema,omitempty"`

	CacheTTL    caddy.Duration `json:"cache_ttl,omitempty"`
	NegCacheTTL caddy.Duration `json:"negative_cache_ttl,omitempty"`

	CacheClearPath string `json:"cache_clear_path,omitempty"`
	CacheControl   string `json:"cache_control,omitempty"`

	pool   *pgxpool.Pool
	cache  *responseCache
	logger *zap.Logger

	customDomainRouteQuery         string
	customDomainPublishedPageQuery string
	customDomainStatusQuery        string

	accountValuesQuery   string
	contactStandardQuery string
	contactCustomQuery   string
	customDomainSEOQuery string
}

type publishedSite struct {
	Title       string
	HTML        string
	Slug        string
	PageSlug    string
	FunnelSlug  string
	FunnelType  string
	UpdatedAt    time.Time
	CacheETag    string
	ResolvedVia  routeKind
	SubAccountID string
}

type customDomainFunnel struct {
	ID              string
	Slug            string
	Status          string
	DomainStatus    string
	Purpose         string
	PageID          string
	PageSlug        string
	PageName        string
	PageStatus      string
	PageIsHomepage  bool
	PageHTMLBytes   int64
	RouteMode       string
	RequestedFunnel string
	RequestedPage   string
	SubAccountID    string
}

func (*Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.db_sites",
		New: func() caddy.Module { return new(Handler) },
	}
}

func (h *Handler) Provision(ctx caddy.Context) error {
	h.logger = ctx.Logger()
	h.loadEnvDefaults()
	if h.DatabaseURL == "" {
		return fmt.Errorf("db_sites: database_url is required")
	}
	if h.CacheClearPath == "" {
		h.CacheClearPath = defaultCacheClearPath
	}
	if h.Schema == "" {
		h.Schema = "public"
	}
	if !safeIdentifier.MatchString(h.Schema) {
		return fmt.Errorf("db_sites: invalid schema %q (must match %s)", h.Schema, safeIdentifier.String())
	}
	h.customDomainRouteQuery = fmt.Sprintf(customDomainRouteSQLTemplate,
		qualifiedTable(h.Schema, "platform_domains"),
		qualifiedTable(h.Schema, "site_funnels"),
		qualifiedTable(h.Schema, "site_pages"),
		qualifiedTable(h.Schema, "site_pages"),
	)
	h.customDomainPublishedPageQuery = fmt.Sprintf(customDomainPublishedPageSQLTemplate, qualifiedTable(h.Schema, "published_sites"))
	h.customDomainStatusQuery = fmt.Sprintf(customDomainStatusSQLTemplate, qualifiedTable(h.Schema, "platform_domains"), qualifiedTable(h.Schema, "site_funnels"))
	h.accountValuesQuery = fmt.Sprintf(accountValuesSQLTemplate, qualifiedTable(h.Schema, "account_custom_values"))
	h.contactStandardQuery = fmt.Sprintf(contactStandardSQLTemplate, qualifiedTable(h.Schema, "contacts"))
	h.contactCustomQuery = fmt.Sprintf(contactCustomSQLTemplate, qualifiedTable(h.Schema, "contact_custom_field_values"), qualifiedTable(h.Schema, "custom_fields"))
	h.customDomainSEOQuery = fmt.Sprintf(customDomainSEOSQLTemplate, qualifiedTable(h.Schema, "platform_domains"), qualifiedTable(h.Schema, "site_funnels"))

	cfg, err := pgxpool.ParseConfig(h.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db_sites: parse database_url: %w", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if _, ok := cfg.ConnConfig.RuntimeParams["search_path"]; !ok {
		cfg.ConnConfig.RuntimeParams["search_path"] = h.Schema
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("db_sites: connect to database: %w", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return fmt.Errorf("db_sites: ping database: %w", err)
	}
	h.pool = pool
	h.cache = newResponseCache()
	h.logDatabaseMetadata(context.Background())
	h.logger.Info("db_sites provisioned",
		zap.String("route_query_version", routeQueryVersion),
		zap.String("database_url", redactDatabaseURL(h.DatabaseURL)),
		zap.String("database_host", cfg.ConnConfig.Host),
		zap.Uint16("database_port", cfg.ConnConfig.Port),
		zap.String("database_name", cfg.ConnConfig.Database),
		zap.String("database_user", cfg.ConnConfig.User),
		zap.String("schema", h.Schema),
		zap.String("search_path", cfg.ConnConfig.RuntimeParams["search_path"]),
		zap.Int32("pool_max_conns", cfg.MaxConns),
		zap.Int32("pool_min_conns", cfg.MinConns),
		zap.Duration("cache_ttl", time.Duration(h.CacheTTL)),
		zap.Duration("negative_cache_ttl", time.Duration(h.NegCacheTTL)),
		zap.String("cache_clear_path", h.CacheClearPath),
		zap.String("cache_control", h.CacheControl),
	)
	return nil
}

func (h *Handler) loadEnvDefaults() {
	envOr := func(field *string, key string) {
		if *field == "" {
			*field = os.Getenv(key)
		}
	}
	envOr(&h.DatabaseURL, "DB_SITES_DATABASE_URL")
	envOr(&h.Schema, "DB_SITES_SCHEMA")
	envOr(&h.CacheClearPath, "DB_SITES_CACHE_CLEAR_PATH")
	envOr(&h.CacheControl, "DB_SITES_CACHE_CONTROL")

	if h.CacheTTL == 0 {
		if s := os.Getenv("DB_SITES_CACHE_TTL"); s != "" {
			if d, err := caddy.ParseDuration(s); err == nil {
				h.CacheTTL = caddy.Duration(d)
			}
		}
	}
	if h.NegCacheTTL == 0 {
		if s := os.Getenv("DB_SITES_NEGATIVE_CACHE_TTL"); s != "" {
			if d, err := caddy.ParseDuration(s); err == nil {
				h.NegCacheTTL = caddy.Duration(d)
			}
		}
	}
}

func (h *Handler) Validate() error {
	if h.pool == nil || h.cache == nil {
		return fmt.Errorf("db_sites: not properly provisioned")
	}
	return nil
}

func (h *Handler) Cleanup() error {
	if h.pool != nil {
		h.pool.Close()
	}
	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	start := time.Now()
	host := effectiveRequestHost(r)
	h.logger.Info("db_sites request received",
		zap.String("method", r.Method),
		zap.String("raw_host", r.Host),
		zap.String("effective_host", host),
		zap.String("path", r.URL.Path),
		zap.String("query", r.URL.RawQuery),
		zap.String("remote_addr", r.RemoteAddr),
		zap.String("user_agent", r.UserAgent()),
		zap.String("x_forwarded_host", r.Header.Get("X-Forwarded-Host")),
		zap.String("x_forwarded_for", r.Header.Get("X-Forwarded-For")),
		zap.String("accept", r.Header.Get("Accept")),
	)
	if host == "" {
		h.logger.Warn("db_sites request rejected: missing host", zap.Duration("duration", time.Since(start)))
		return caddyhttp.Error(http.StatusBadRequest, fmt.Errorf("missing Host header"))
	}
	if strings.EqualFold(pathClean(r.URL.Path), pathClean(h.CacheClearPath)) {
		h.logger.Info("db_sites cache clear requested", zap.String("host", host), zap.String("path", r.URL.Path))
		return h.serveCacheClear(w, r)
	}
	if kind := seoFileKind(r.URL.Path); kind != "" {
		h.logger.Info("db_sites seo file requested", zap.String("host", host), zap.String("kind", kind))
		return h.serveSEOFile(w, r, host, kind)
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		h.logger.Warn("db_sites request rejected: method not allowed",
			zap.String("method", r.Method),
			zap.String("host", host),
			zap.Duration("duration", time.Since(start)),
		)
		return caddyhttp.Error(http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
	}

	target := resolveTarget(host, r.URL.Path)
	h.logger.Info("db_sites route resolved",
		zap.String("host", host),
		zap.String("route_kind", string(target.Kind)),
		zap.String("request_path", target.RequestPath),
		zap.String("page_slug", target.PageSlug),
		zap.String("schema", h.Schema),
	)

	site, found, code, err := h.lookupPublishedSite(r.Context(), target)
	if err != nil {
		h.logger.Error("db_sites request failed: published site lookup error",
			zap.String("host", host),
			zap.String("page_slug", target.PageSlug),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return caddyhttp.Error(http.StatusBadGateway, err)
	}
	if !found {
		h.logger.Info("db_sites request completed: no published html",
			zap.String("host", host),
			zap.String("raw_page_slug", target.PageSlug),
			zap.Int("status", code),
			zap.Duration("duration", time.Since(start)),
		)
		return caddyhttp.Error(code, fmt.Errorf(http.StatusText(code)))
	}

	// Resolve custom-field / custom-value merge tokens per request. The cached
	// copy keeps raw tokens, so editing a value takes effect without republishing.
	// Token-free pages skip this entirely (no extra query, cached ETag stays stable).
	served := site
	if htmlHasMergeTokens(site.HTML) {
		values := h.buildMergeValues(r.Context(), site.SubAccountID, r.URL.Query().Get("cid"))
		resolved := replaceMergeTokens(site.HTML, values)
		if resolved != site.HTML {
			cp := *site
			cp.HTML = resolved
			cp.CacheETag = weakETag(resolved)
			served = &cp
		}
	}

	status := http.StatusOK
	if etagMatches(r, served.CacheETag) {
		status = http.StatusNotModified
	}
	h.logger.Info("db_sites request completed: serving html",
		zap.String("host", host),
		zap.String("raw_page_slug", target.PageSlug),
		zap.String("normalized_page_slug", served.PageSlug),
		zap.String("funnel_slug", served.FunnelSlug),
		zap.String("published_slug", served.Slug),
		zap.String("title", served.Title),
		zap.String("funnel_type", served.FunnelType),
		zap.Time("published_updated_at", served.UpdatedAt),
		zap.Int("html_bytes", len(served.HTML)),
		zap.String("etag", served.CacheETag),
		zap.Int("status", status),
		zap.Duration("duration", time.Since(start)),
	)
	writeHTML(w, r, served, h.CacheControl)
	return nil
}

func (h *Handler) lookupPublishedSite(ctx context.Context, target routeTarget) (*publishedSite, bool, int, error) {
	if target.Kind == routeCustomDomain {
		return h.lookupCustomDomainPublishedSite(ctx, target)
	}

	key := cacheKey(target)
	if site, found, hit, code := h.cache.get(key); hit {
		h.logger.Info("db_sites cache hit",
			zap.String("cache_key", key),
			zap.Bool("found", found),
			zap.Int("cached_status", code),
			zap.String("host", target.Host),
			zap.String("page_slug", target.PageSlug),
		)
		return site, found, code, nil
	}
	h.logger.Info("db_sites cache miss",
		zap.String("cache_key", key),
		zap.String("host", target.Host),
		zap.String("page_slug", target.PageSlug),
	)

	site, found, code, err := h.queryPublishedSite(ctx, target)
	if err != nil {
		return nil, false, 0, err
	}
	if found {
		ttl := resolveTTL(time.Duration(h.CacheTTL))
		h.cache.set(key, site, true, ttl, http.StatusOK)
		h.logger.Info("db_sites cache store positive",
			zap.String("cache_key", key),
			zap.Duration("ttl", ttl),
			zap.String("published_slug", site.Slug),
		)
		return site, true, http.StatusOK, nil
	}

	ttl := time.Duration(h.NegCacheTTL)
	if ttl <= 0 {
		ttl = resolveTTL(time.Duration(h.CacheTTL))
	}
	h.cache.set(key, nil, false, ttl, code)
	h.logger.Info("db_sites cache store negative",
		zap.String("cache_key", key),
		zap.Duration("ttl", ttl),
		zap.Int("status", code),
	)
	return nil, false, code, nil
}

func (h *Handler) queryPublishedSite(ctx context.Context, target routeTarget) (*publishedSite, bool, int, error) {
	switch target.Kind {
	default:
		return nil, false, http.StatusNotFound, fmt.Errorf("unknown route kind %q", target.Kind)
	}
}

func (h *Handler) lookupCustomDomainPublishedSite(ctx context.Context, target routeTarget) (*publishedSite, bool, int, error) {
	funnel, found, code, err := h.lookupCustomDomainFunnel(ctx, target)
	if err != nil || !found {
		return nil, false, code, err
	}

	rawPageSlug := target.PageSlug
	normalizedTarget := target
	normalizedTarget.PageSlug = funnel.PageSlug
	publishedSlug := funnel.Slug + "--" + normalizedTarget.PageSlug

	h.logger.Info("db_sites custom domain path normalized",
		zap.String("host", target.Host),
		zap.String("original_path", target.RequestPath),
		zap.String("raw_page_slug", rawPageSlug),
		zap.String("funnel_slug", funnel.Slug),
		zap.String("route_mode", funnel.RouteMode),
		zap.String("requested_funnel_slug", funnel.RequestedFunnel),
		zap.String("requested_page_slug", funnel.RequestedPage),
		zap.String("normalized_page_slug", normalizedTarget.PageSlug),
		zap.String("expected_published_slug", publishedSlug),
	)

	// Include the funnel id in the cache key: two funnels on the same custom
	// domain can share a page slug (e.g. both have "index"), so a host+slug key
	// alone collides and serves the wrong funnel's page.
	key := cacheKey(normalizedTarget) + "|" + funnel.ID
	if site, found, hit, code := h.cache.get(key); hit {
		h.logger.Info("db_sites cache hit",
			zap.String("cache_key", key),
			zap.Bool("found", found),
			zap.Int("cached_status", code),
			zap.String("host", normalizedTarget.Host),
			zap.String("original_path", target.RequestPath),
			zap.String("raw_page_slug", rawPageSlug),
			zap.String("normalized_page_slug", normalizedTarget.PageSlug),
			zap.String("funnel_slug", funnel.Slug),
			zap.String("route_mode", funnel.RouteMode),
		)
		return site, found, code, nil
	}
	h.logger.Info("db_sites cache miss",
		zap.String("cache_key", key),
		zap.String("host", normalizedTarget.Host),
		zap.String("original_path", target.RequestPath),
		zap.String("raw_page_slug", rawPageSlug),
		zap.String("normalized_page_slug", normalizedTarget.PageSlug),
		zap.String("funnel_slug", funnel.Slug),
		zap.String("route_mode", funnel.RouteMode),
	)

	site, found, code, err := h.queryCustomDomainPublishedPage(ctx, normalizedTarget, funnel, rawPageSlug, publishedSlug)
	if err != nil {
		return nil, false, 0, err
	}
	if found {
		ttl := resolveTTL(time.Duration(h.CacheTTL))
		h.cache.set(key, site, true, ttl, http.StatusOK)
		h.logger.Info("db_sites cache store positive",
			zap.String("cache_key", key),
			zap.Duration("ttl", ttl),
			zap.String("published_slug", site.Slug),
			zap.String("normalized_page_slug", normalizedTarget.PageSlug),
			zap.String("funnel_slug", funnel.Slug),
			zap.String("route_mode", funnel.RouteMode),
		)
		return site, true, http.StatusOK, nil
	}

	ttl := time.Duration(h.NegCacheTTL)
	if ttl <= 0 {
		ttl = resolveTTL(time.Duration(h.CacheTTL))
	}
	h.cache.set(key, nil, false, ttl, code)
	h.logger.Info("db_sites cache store negative",
		zap.String("cache_key", key),
		zap.Duration("ttl", ttl),
		zap.Int("status", code),
		zap.String("normalized_page_slug", normalizedTarget.PageSlug),
		zap.String("funnel_slug", funnel.Slug),
		zap.String("route_mode", funnel.RouteMode),
	)
	return nil, false, code, nil
}

func (h *Handler) queryCustomDomainPublishedPage(ctx context.Context, target routeTarget, funnel customDomainFunnel, rawPageSlug, publishedSlug string) (*publishedSite, bool, int, error) {
	var site publishedSite
	site.ResolvedVia = target.Kind
	site.PageSlug = target.PageSlug
	site.FunnelSlug = funnel.Slug
	site.SubAccountID = funnel.SubAccountID
	row := h.pool.QueryRow(ctx, h.customDomainPublishedPageQuery, funnel.ID, publishedSlug)
	if err := row.Scan(&site.Slug, &site.Title, &site.FunnelType, &site.HTML, &site.UpdatedAt); err != nil {
		if err == pgx.ErrNoRows {
			h.logger.Info("db_sites published html query returned no rows",
				zap.String("host", target.Host),
				zap.String("request_path", target.RequestPath),
				zap.String("raw_page_slug", rawPageSlug),
				zap.String("normalized_page_slug", target.PageSlug),
				zap.String("funnel_slug", funnel.Slug),
				zap.String("expected_published_slug", publishedSlug),
			)
			h.logDomainDiagnostics(ctx, target)
			return nil, false, http.StatusNotFound, nil
		}
		return nil, false, 0, fmt.Errorf("query published site: %w", err)
	}
	site.CacheETag = weakETag(site.HTML)
	h.logger.Info("db_sites published html row found",
		zap.String("host", target.Host),
		zap.String("request_path", target.RequestPath),
		zap.String("raw_page_slug", rawPageSlug),
		zap.String("normalized_page_slug", target.PageSlug),
		zap.String("funnel_slug", funnel.Slug),
		zap.String("published_slug", site.Slug),
		zap.Int("html_bytes", len(site.HTML)),
		zap.Time("updated_at", site.UpdatedAt),
	)
	return &site, true, http.StatusOK, nil
}

func (h *Handler) lookupCustomDomainFunnel(ctx context.Context, target routeTarget) (customDomainFunnel, bool, int, error) {
	h.logger.Info("db_sites query custom domain funnel",
		zap.String("schema", h.Schema),
		zap.String("host", target.Host),
		zap.String("request_path", target.RequestPath),
		zap.String("raw_page_slug", target.PageSlug),
	)

	var funnel customDomainFunnel
	firstSegment, secondSegment := firstTwoPathSegments(target.RequestPath)
	err := h.pool.QueryRow(ctx, h.customDomainRouteQuery, target.Host, firstSegment, secondSegment).Scan(
		&funnel.ID,
		&funnel.Slug,
		&funnel.Status,
		&funnel.DomainStatus,
		&funnel.Purpose,
		&funnel.PageID,
		&funnel.PageSlug,
		&funnel.PageName,
		&funnel.PageStatus,
		&funnel.PageIsHomepage,
		&funnel.PageHTMLBytes,
		&funnel.RouteMode,
		&funnel.SubAccountID,
	)
	if err == pgx.ErrNoRows {
		h.logger.Info("db_sites custom domain funnel query returned no rows",
			zap.String("host", target.Host),
		)
		h.logDomainDiagnostics(ctx, target)
		code, err := h.notFoundStatus(ctx, target)
		return customDomainFunnel{}, false, code, err
	}
	if err != nil {
		h.logger.Error("db_sites custom domain route query failed",
			zap.String("route_query_version", routeQueryVersion),
			zap.String("host", target.Host),
			zap.String("first_path_segment", firstSegment),
			zap.String("second_path_segment", secondSegment),
			zap.Error(err),
		)
		return customDomainFunnel{}, false, 0, fmt.Errorf("query custom domain funnel (%s): %w", routeQueryVersion, err)
	}

	h.logger.Info("db_sites custom domain funnel found",
		zap.String("host", target.Host),
		zap.String("funnel_id", funnel.ID),
		zap.String("funnel_slug", funnel.Slug),
		zap.String("funnel_status", funnel.Status),
		zap.String("domain_status", funnel.DomainStatus),
		zap.String("purpose", funnel.Purpose),
		zap.String("requested_funnel_slug", firstSegment),
		zap.String("requested_page_slug", secondSegment),
		zap.String("route_mode", funnel.RouteMode),
		zap.String("site_page_id", funnel.PageID),
		zap.String("site_page_slug", funnel.PageSlug),
		zap.String("site_page_name", funnel.PageName),
		zap.String("site_page_status", funnel.PageStatus),
		zap.Bool("site_page_is_homepage", funnel.PageIsHomepage),
		zap.Int64("site_page_html_bytes", funnel.PageHTMLBytes),
	)
	funnel.RequestedFunnel = firstSegment
	funnel.RequestedPage = secondSegment

	if funnel.DomainStatus != "verified" || (funnel.Purpose != "" && funnel.Purpose != "funnel") {
		return customDomainFunnel{}, false, http.StatusForbidden, nil
	}
	if funnel.Status != "published" {
		return customDomainFunnel{}, false, http.StatusLocked, nil
	}
	return funnel, true, http.StatusOK, nil
}

func (h *Handler) notFoundStatus(ctx context.Context, target routeTarget) (int, error) {
	if target.Kind == routeCustomDomain {
		return h.customDomainMissStatus(ctx, target.Host)
	}
	return http.StatusNotFound, nil
}

func (h *Handler) customDomainMissStatus(ctx context.Context, host string) (int, error) {
	var domainStatus, purpose string
	var siteStatus sql.NullString
	h.logger.Info("db_sites checking custom domain miss status",
		zap.String("host", host),
		zap.String("schema", h.Schema),
	)
	err := h.pool.QueryRow(ctx, h.customDomainStatusQuery, host).Scan(&domainStatus, &purpose, &siteStatus)
	if err == pgx.ErrNoRows {
		h.logger.Info("db_sites custom domain status: domain not registered",
			zap.String("host", host),
			zap.Int("status", http.StatusNotFound),
		)
		return http.StatusNotFound, nil
	}
	if err != nil {
		return 0, fmt.Errorf("query custom domain status: %w", err)
	}
	if domainStatus != "verified" || (purpose != "" && purpose != "funnel") {
		h.logger.Info("db_sites custom domain status: forbidden",
			zap.String("host", host),
			zap.String("domain_status", domainStatus),
			zap.String("purpose", purpose),
			zap.Int("status", http.StatusForbidden),
		)
		return http.StatusForbidden, nil
	}
	if siteStatus.Valid && siteStatus.String != "published" {
		h.logger.Info("db_sites custom domain status: site locked",
			zap.String("host", host),
			zap.String("site_status", siteStatus.String),
			zap.Int("status", http.StatusLocked),
		)
		return http.StatusLocked, nil
	}
	h.logger.Info("db_sites custom domain status: domain valid but page/html missing",
		zap.String("host", host),
		zap.String("domain_status", domainStatus),
		zap.String("purpose", purpose),
		zap.String("site_status", nullStringValue(siteStatus)),
		zap.Int("status", http.StatusNotFound),
	)
	return http.StatusNotFound, nil
}

func (h *Handler) serveCacheClear(w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodHead:
	default:
		w.Header().Set("Allow", "GET, HEAD, POST, DELETE")
		return caddyhttp.Error(http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
	}
	removed := h.cache.invalidateAll()
	h.logger.Info("db_sites cache cleared",
		zap.Int("removed_entries", removed),
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
	)
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return nil
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(struct {
		OK      bool `json:"ok"`
		Removed int  `json:"removed"`
	}{OK: true, Removed: removed})
	return nil
}

func writeHTML(w http.ResponseWriter, r *http.Request, site *publishedSite, cacheControl string) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("ETag", site.CacheETag)
	h.Set("X-Site-Slug", site.Slug)
	h.Set("X-Site-Resolved-Via", string(site.ResolvedVia))
	if cacheControl != "" {
		h.Set("Cache-Control", cacheControl)
	}
	if etagMatches(r, site.CacheETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(site.HTML))
	}
}

func etagMatches(r *http.Request, etag string) bool {
	return r.Header.Get("If-None-Match") == etag
}

func cacheKey(target routeTarget) string {
	return string(target.Kind) + "|" + target.Host + "|" + target.PageSlug
}

func weakETag(s string) string {
	sum := sha256.Sum256([]byte(s))
	return `W/"` + hex.EncodeToString(sum[:12]) + `"`
}

func redactDatabaseURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(invalid database_url)"
	}
	return u.Redacted()
}

func pathClean(p string) string {
	if p == "" {
		return "/"
	}
	return "/" + strings.TrimPrefix(strings.TrimSuffix(p, "/"), "/")
}

func qualifiedTable(schema, table string) string {
	return quoteIdent(schema) + "." + quoteIdent(table)
}

func quoteIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func nullStringValue(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func (h *Handler) logDatabaseMetadata(ctx context.Context) {
	var dbName, currentSchema, searchPath, version string
	err := h.pool.QueryRow(ctx, `SELECT current_database(), current_schema(), current_setting('search_path'), version()`).Scan(&dbName, &currentSchema, &searchPath, &version)
	if err != nil {
		h.logger.Warn("db_sites database metadata query failed", zap.Error(err))
		return
	}
	h.logger.Info("db_sites database connected",
		zap.String("database", dbName),
		zap.String("configured_schema", h.Schema),
		zap.String("current_schema", currentSchema),
		zap.String("search_path", searchPath),
		zap.String("postgres_version", version),
	)
}

func (h *Handler) logDomainDiagnostics(ctx context.Context, target routeTarget) {
	rows, err := h.pool.Query(ctx, fmt.Sprintf(domainDiagnosticsSQLTemplate,
		qualifiedTable(h.Schema, "platform_domains"),
		qualifiedTable(h.Schema, "site_funnels"),
		qualifiedTable(h.Schema, "site_pages"),
		qualifiedTable(h.Schema, "published_sites"),
	), target.Host, target.PageSlug)
	if err != nil {
		h.logger.Warn("db_sites 404 diagnostics query failed",
			zap.String("host", target.Host),
			zap.String("page_slug", target.PageSlug),
			zap.Error(err),
		)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var d domainDiagnostic
		if err := rows.Scan(
			&d.PlatformDomain,
			&d.PlatformDomainStatus,
			&d.PlatformDomainPurpose,
			&d.FunnelID,
			&d.FunnelSlug,
			&d.FunnelStatus,
			&d.FunnelDomain,
			&d.PlatformDomainID,
			&d.SitePageID,
			&d.SitePageSlug,
			&d.SitePageName,
			&d.SitePageIsHomepage,
			&d.SitePageStatus,
			&d.SitePageHTMLBytes,
			&d.SitePageUpdatedAt,
			&d.ExpectedPublishedSlug,
			&d.PublishedSlug,
			&d.PublishedCustomDomain,
			&d.HTMLBytes,
		); err != nil {
			h.logger.Warn("db_sites 404 diagnostics scan failed",
				zap.String("host", target.Host),
				zap.Error(err),
			)
			return
		}
		h.logger.Info("db_sites 404 diagnostics candidate",
			zap.String("host", target.Host),
			zap.String("page_slug", target.PageSlug),
			zap.String("platform_domain", nullStringValue(d.PlatformDomain)),
			zap.String("platform_domain_status", nullStringValue(d.PlatformDomainStatus)),
			zap.String("platform_domain_purpose", nullStringValue(d.PlatformDomainPurpose)),
			zap.String("funnel_id", nullStringValue(d.FunnelID)),
			zap.String("funnel_slug", nullStringValue(d.FunnelSlug)),
			zap.String("funnel_status", nullStringValue(d.FunnelStatus)),
			zap.String("funnel_domain", nullStringValue(d.FunnelDomain)),
			zap.String("platform_domain_id", nullStringValue(d.PlatformDomainID)),
			zap.String("site_page_id", nullStringValue(d.SitePageID)),
			zap.String("site_page_slug", nullStringValue(d.SitePageSlug)),
			zap.String("site_page_name", nullStringValue(d.SitePageName)),
			zap.Bool("site_page_is_homepage", nullBoolValue(d.SitePageIsHomepage)),
			zap.String("site_page_status", nullStringValue(d.SitePageStatus)),
			zap.Int64("site_page_html_bytes", nullInt64Value(d.SitePageHTMLBytes)),
			zap.String("site_page_updated_at", nullTimeValue(d.SitePageUpdatedAt)),
			zap.String("expected_published_slug", nullStringValue(d.ExpectedPublishedSlug)),
			zap.String("published_slug", nullStringValue(d.PublishedSlug)),
			zap.String("published_custom_domain", nullStringValue(d.PublishedCustomDomain)),
			zap.Int64("html_bytes", nullInt64Value(d.HTMLBytes)),
		)
	}
	if err := rows.Err(); err != nil {
		h.logger.Warn("db_sites 404 diagnostics rows failed",
			zap.String("host", target.Host),
			zap.Error(err),
		)
		return
	}
	if count == 0 {
		h.logger.Info("db_sites 404 diagnostics found no platform_domain/site candidates",
			zap.String("host", target.Host),
			zap.String("page_slug", target.PageSlug),
		)
	}
}

func nullInt64Value(v sql.NullInt64) int64 {
	if !v.Valid {
		return 0
	}
	return v.Int64
}

func nullBoolValue(v sql.NullBool) bool {
	if !v.Valid {
		return false
	}
	return v.Bool
}

func nullTimeValue(v sql.NullTime) string {
	if !v.Valid {
		return ""
	}
	return v.Time.Format(time.RFC3339)
}

type domainDiagnostic struct {
	PlatformDomain        sql.NullString
	PlatformDomainStatus  sql.NullString
	PlatformDomainPurpose sql.NullString
	FunnelID              sql.NullString
	FunnelSlug            sql.NullString
	FunnelStatus          sql.NullString
	FunnelDomain          sql.NullString
	PlatformDomainID      sql.NullString
	SitePageID            sql.NullString
	SitePageSlug          sql.NullString
	SitePageName          sql.NullString
	SitePageIsHomepage    sql.NullBool
	SitePageStatus        sql.NullString
	SitePageHTMLBytes     sql.NullInt64
	SitePageUpdatedAt     sql.NullTime
	ExpectedPublishedSlug sql.NullString
	PublishedSlug         sql.NullString
	PublishedCustomDomain sql.NullString
	HTMLBytes             sql.NullInt64
}

const customDomainRouteSQLTemplate = `
WITH candidates AS (
	SELECT
		sf.id,
		sf.slug,
		sf.serve_at_root,
		sf.sub_account_id,
		sf.status AS funnel_status,
		pd.status AS domain_status,
		COALESCE(pd.purpose, '') AS purpose,
		CASE WHEN sf.slug = $2 THEN true ELSE false END AS funnel_slug_matched
	FROM %s pd
	JOIN %s sf ON sf.platform_domain_id = pd.id
	WHERE lower(pd.domain) = lower($1)
	   OR lower(sf.domain) = lower($1)
	   OR (pd.include_www AND lower('www.' || pd.domain) = lower($1))
),
selected_funnel AS (
	SELECT
		candidates.id AS funnel_id,
		candidates.slug AS funnel_slug,
		candidates.funnel_status AS funnel_status,
		candidates.domain_status AS domain_status,
		candidates.purpose AS purpose,
		candidates.funnel_slug_matched AS funnel_slug_matched,
		candidates.sub_account_id AS sub_account_id,
		CASE
			WHEN candidates.funnel_slug_matched THEN NULLIF($3, '')
			ELSE NULLIF($2, '')
		END AS requested_page_slug,
		CASE
			WHEN candidates.funnel_slug_matched THEN 'funnel_slug_prefix'
			ELSE 'domain_default'
		END AS route_mode
	FROM candidates
	JOIN %s sp ON sp.funnel_id = candidates.id
	WHERE (
		CASE
			WHEN candidates.funnel_slug_matched THEN NULLIF($3, '')
			ELSE NULLIF($2, '')
		END IS NULL
		AND sp.is_homepage = true
	)
	OR (
		CASE
			WHEN candidates.funnel_slug_matched THEN NULLIF($3, '')
			ELSE NULLIF($2, '')
		END IS NOT NULL
		AND sp.slug = CASE
			WHEN candidates.funnel_slug_matched THEN NULLIF($3, '')
			ELSE NULLIF($2, '')
		END
	)
	ORDER BY
		CASE WHEN candidates.funnel_slug_matched THEN 0 ELSE 1 END,
		CASE WHEN candidates.serve_at_root THEN 0 ELSE 1 END,
		CASE WHEN candidates.funnel_status = 'published' THEN 0 ELSE 1 END,
		CASE WHEN sp.status = 'published' THEN 0 ELSE 1 END,
		sp.is_homepage DESC,
		sp.sort_order,
		sp.updated_at DESC,
		candidates.slug
	LIMIT 1
)
SELECT
	sf.funnel_id::text,
	sf.funnel_slug,
	sf.funnel_status,
	sf.domain_status,
	sf.purpose,
	sp.id::text,
	sp.slug,
	COALESCE(sp.name, ''),
	sp.status,
	sp.is_homepage,
	COALESCE(LENGTH(sp.html_content), 0),
	sf.route_mode,
	sf.sub_account_id::text
FROM selected_funnel sf
JOIN %s sp ON sp.funnel_id = sf.funnel_id
WHERE (
	sf.requested_page_slug IS NULL
	AND sp.is_homepage = true
)
OR (
	sf.requested_page_slug IS NOT NULL
	AND sp.slug = sf.requested_page_slug
)
ORDER BY
	CASE WHEN sp.status = 'published' THEN 0 ELSE 1 END,
	sp.is_homepage DESC,
	sp.sort_order,
	sp.updated_at DESC
LIMIT 1`

const customDomainPublishedPageSQLTemplate = `
SELECT
	ps.slug,
	COALESCE(ps.title, ''),
	COALESCE(ps.funnel_type, ''),
	ps.html_content,
	ps.updated_at
FROM %s ps
WHERE ps.funnel_id::text = $1
  AND ps.slug = $2
  AND ps.html_content IS NOT NULL
LIMIT 1`

const customDomainStatusSQLTemplate = `
SELECT
	pd.status,
	COALESCE(pd.purpose, ''),
	sf.status
FROM %s pd
LEFT JOIN %s sf ON sf.platform_domain_id = pd.id
WHERE lower(pd.domain) = lower($1)
   OR (pd.include_www AND lower('www.' || pd.domain) = lower($1))
LIMIT 1`

const domainDiagnosticsSQLTemplate = `
SELECT
	pd.domain,
	pd.status,
	pd.purpose,
	sf.id::text,
	sf.slug,
	sf.status,
	sf.domain,
	sf.platform_domain_id::text,
	sp.id::text,
	sp.slug,
	sp.name,
	sp.is_homepage,
	sp.status,
	LENGTH(sp.html_content),
	sp.updated_at,
	CASE
		WHEN sf.slug IS NULL THEN NULL
		WHEN COALESCE(sp.slug, $2) = '' THEN sf.slug || '--index'
		ELSE sf.slug || '--' || COALESCE(sp.slug, $2)
	END AS expected_published_slug,
	ps.slug,
	ps.custom_domain,
	LENGTH(ps.html_content)
FROM %s pd
LEFT JOIN %s sf ON sf.platform_domain_id = pd.id
LEFT JOIN %s sp ON sp.funnel_id = sf.id AND (sp.slug = $2 OR ($2 = 'index' AND sp.is_homepage = true))
LEFT JOIN %s ps ON ps.funnel_id = sf.id AND ps.slug = sf.slug || '--' || COALESCE(sp.slug, $2)
WHERE lower(pd.domain) = lower($1)
   OR lower(sf.domain) = lower($1)
   OR lower(ps.custom_domain) = lower($1)
   OR (pd.include_www AND lower('www.' || pd.domain) = lower($1))
LIMIT 10`

var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
)
