package dbsites

import "testing"

func TestReplaceMergeTokens(t *testing.T) {
	values := map[string]string{
		"company.name":       "acme Inc",
		"contact.first_name": "acme",
	}
	cases := map[string]string{
		"<h1>{{company.name}}</h1>":                 "<h1>acme Inc</h1>",
		"Hi {{ contact.first_name }}!":              "acme!",
		"{%company.name%}":                          "acme Inc",
		"<p>{{company.unknown}}</p>":                "<p></p>",                        // unresolved merge token → blank
		"<code>{{ notAToken() }}</code>":            "<code>{{ notAToken() }}</code>", // non-merge braces left as-is
		`<span data-merge-token="company.name" class="merge-token-pill">{{company.name}}</span>`: "acme Inc",
	}
	for in, want := range cases {
		if got := replaceMergeTokens(in, values); got != want {
			t.Fatalf("replaceMergeTokens(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHtmlHasMergeTokens(t *testing.T) {
	if htmlHasMergeTokens("<p>no tokens here</p>") {
		t.Fatal("expected no tokens")
	}
	if !htmlHasMergeTokens("<p>{{company.name}}</p>") {
		t.Fatal("expected tokens")
	}
}
