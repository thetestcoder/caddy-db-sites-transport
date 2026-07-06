package dbsites

import (
	"context"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)


const accountValuesSQLTemplate = `SELECT value_key, COALESCE(value_text, '') FROM %s WHERE sub_account_id = $1`

const contactStandardSQLTemplate = `SELECT COALESCE(first_name, ''), COALESCE(last_name, ''), COALESCE(email, ''), COALESCE(phone, ''), COALESCE(company, ''), COALESCE(display_name, '') FROM %s WHERE id = $1 AND sub_account_id = $2 LIMIT 1`

const contactCustomSQLTemplate = `SELECT cf.field_slug, COALESCE(v.value, '') FROM %s v JOIN %s cf ON cf.id = v.field_id WHERE v.contact_id = $1 AND cf.sub_account_id = $2`

var (
	mergePillRe    = regexp.MustCompile(`(?is)<span\b[^>]*\bdata-merge-token="([^"]*)"[^>]*>.*?</span>`)
	mergePercentRe = regexp.MustCompile(`\{%\s*([^%]+?)\s*%\}`)
	mergeTokenRe   = regexp.MustCompile(`\{\{\s*([^}]+?)\s*\}\}`)
	mergePathRe    = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)
	uuidRe         = regexp.MustCompile(`^[0-9a-fA-F-]{32,36}$`)
)

func htmlHasMergeTokens(html string) bool {
	return strings.Contains(html, "{{") || strings.Contains(html, "data-merge-token")
}

// normalizes {% %} to {{ }}.
func prepareHTMLForMerge(html string) string {
	html = mergePillRe.ReplaceAllStringFunc(html, func(m string) string {
		sub := mergePillRe.FindStringSubmatch(m)
		path := strings.TrimSpace(sub[1])
		if path == "" {
			return ""
		}
		return "{{" + path + "}}"
	})
	return mergePercentRe.ReplaceAllString(html, "{{$1}}")
}


func replaceMergeTokens(html string, values map[string]string) string {
	if html == "" {
		return html
	}
	html = prepareHTMLForMerge(html)
	return mergeTokenRe.ReplaceAllStringFunc(html, func(m string) string {
		sub := mergeTokenRe.FindStringSubmatch(m)
		key := strings.TrimSpace(sub[1])
		if v, ok := values[key]; ok {
			return v
		}
		if mergePathRe.MatchString(key) {
			return ""
		}
		return m
	})
}


func (h *Handler) buildMergeValues(ctx context.Context, subAccountID, contactID string) map[string]string {
	values := make(map[string]string)
	if subAccountID == "" {
		return values
	}

	rows, err := h.pool.Query(ctx, h.accountValuesQuery, subAccountID)
	if err != nil {
		h.logger.Warn("db_sites account values query failed", zap.String("sub_account_id", subAccountID), zap.Error(err))
	} else {
		for rows.Next() {
			var key, val string
			if scanErr := rows.Scan(&key, &val); scanErr == nil {
				if val == "" && (strings.HasPrefix(key, "contact.") || strings.HasPrefix(key, "opportunity.")) {
					continue
				}
				values[key] = val
			}
		}
		rows.Close()
	}

	if contactID != "" && uuidRe.MatchString(contactID) {
		h.addContactValues(ctx, values, subAccountID, contactID)
	}
	return values
}

func (h *Handler) addContactValues(ctx context.Context, values map[string]string, subAccountID, contactID string) {
	set := func(slug, v string) {
		values["contact."+slug] = v
		if _, ok := values[slug]; !ok {
			values[slug] = v 
		}
	}

	var first, last, email, phone, company, display string
	err := h.pool.QueryRow(ctx, h.contactStandardQuery, contactID, subAccountID).
		Scan(&first, &last, &email, &phone, &company, &display)
	if err == nil {
		set("first_name", first)
		set("last_name", last)
		set("email", email)
		set("phone", phone)
		set("company", company)
		set("display_name", display)
		name := strings.TrimSpace(first + " " + last)
		if name == "" {
			name = display
		}
		set("name", name)
	} else if err != pgx.ErrNoRows {
		h.logger.Warn("db_sites contact standard query failed", zap.String("contact_id", contactID), zap.Error(err))
	}

	crows, err := h.pool.Query(ctx, h.contactCustomQuery, contactID, subAccountID)
	if err != nil {
		h.logger.Warn("db_sites contact custom query failed", zap.String("contact_id", contactID), zap.Error(err))
		return
	}
	defer crows.Close()
	for crows.Next() {
		var slug, val string
		if scanErr := crows.Scan(&slug, &val); scanErr == nil && slug != "" {
			set(slug, val)
		}
	}
}
