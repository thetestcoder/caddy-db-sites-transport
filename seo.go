package dbsites

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// robots.txt / sitemap.xml are served at the custom-domain root for SEO. They
// live on site_funnels (robots_txt / sitemap_xml), set per funnel in the app.
// Only the dedicated (custom-domain) serving plane needs them, so they're handled
// here rather than in the app. Resolves the domain's primary published funnel
// (serve_at_root first, then most-recently-updated) and serves its content.
const customDomainSEOSQLTemplate = `
SELECT COALESCE(sf.robots_txt, ''), COALESCE(sf.sitemap_xml, '')
FROM %s pd
JOIN %s sf ON sf.platform_domain_id = pd.id
WHERE (lower(pd.domain) = lower($1) OR (pd.include_www AND lower('www.' || pd.domain) = lower($1)))
  AND pd.status = 'verified'
  AND (pd.purpose IS NULL OR pd.purpose = 'funnel')
  AND sf.status = 'published'
ORDER BY
  CASE WHEN sf.serve_at_root THEN 0 ELSE 1 END,
  sf.updated_at DESC NULLS LAST
LIMIT 1`

// seoFileKind returns "robots" or "sitemap" for the two SEO paths, else "".
func seoFileKind(p string) string {
	switch strings.ToLower(pathClean(p)) {
	case "/robots.txt":
		return "robots"
	case "/sitemap.xml":
		return "sitemap"
	}
	return ""
}

func (h *Handler) serveSEOFile(w http.ResponseWriter, r *http.Request, host, kind string) error {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		return caddyhttp.Error(http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
	}

	var robots, sitemap string
	err := h.pool.QueryRow(r.Context(), h.customDomainSEOQuery, host).Scan(&robots, &sitemap)
	if err == pgx.ErrNoRows {
		return caddyhttp.Error(http.StatusNotFound, fmt.Errorf(http.StatusText(http.StatusNotFound)))
	}
	if err != nil {
		h.logger.Error("db_sites seo query failed", zap.String("host", host), zap.String("kind", kind), zap.Error(err))
		return caddyhttp.Error(http.StatusBadGateway, err)
	}

	body, contentType := sitemap, "application/xml; charset=utf-8"
	if kind == "robots" {
		body, contentType = robots, "text/plain; charset=utf-8"
	}
	if strings.TrimSpace(body) == "" {
		// Not configured for this domain — nothing to serve.
		return caddyhttp.Error(http.StatusNotFound, fmt.Errorf(http.StatusText(http.StatusNotFound)))
	}

	h.logger.Info("db_sites seo file served", zap.String("host", host), zap.String("kind", kind), zap.Int("bytes", len(body)))
	w.Header().Set("Content-Type", contentType)
	if h.CacheControl != "" {
		w.Header().Set("Cache-Control", h.CacheControl)
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(body))
	}
	return nil
}
