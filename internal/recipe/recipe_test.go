package recipe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommittedRecipesValidate(t *testing.T) {
	t.Setenv("MOCKSOURCES_URL", "http://mocksources:8081")
	set, err := Load(filepath.Join("..", "..", "recipes"))
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Recipes) != 8 {
		t.Fatalf("expected 8 recipes, got %d", len(set.Recipes))
	}
	for _, r := range set.Recipes {
		if !strings.HasPrefix(r.URL, "http://mocksources:8081/") {
			t.Errorf("%s: url env not expanded: %s", r.SourceID, r.URL)
		}
	}
	if set.SHA256 == "" {
		t.Fatal("sha missing")
	}
}

func minimal() string {
	return `{"source_id":"x","carrier":"X","url":"http://h/p","format":"csv",
	 "columns":{"origin_raw":"o","destination_raw":"d","container_type_raw":"t","price_raw":"p","currency":"c","unit":"per_container","valid_from_raw":"f","valid_to_raw":"e"}}`
}

func TestDefaultsFilled(t *testing.T) {
	r, err := Parse([]byte(minimal()))
	if err != nil {
		t.Fatal(err)
	}
	if r.Compression != "none" || r.Auth.Type != "none" || r.Pagination.Type != "none" ||
		r.Retry.MaxAttempts != 4 || r.Timeout.String() != "20s" || r.Hints.DecimalStyle != "us" {
		t.Fatalf("defaults: %+v", r)
	}
}

func TestValidationErrorsNameTheField(t *testing.T) {
	cases := map[string]string{
		`"source_id":"Bad ID"`:                             "source_id",
		`"carrier":"lower"`:                                "carrier",
		`"url":"ftp://x"`:                                  "url",
		`"format":"xml"`:                                   "format",
		`"compression":"zip"`:                              "compression",
		`"auth":{"type":"api_key"}`:                        "auth",
		`"pagination":{"type":"page"}`:                     "pagination",
		`"format":"json"`:                                  "records_path",
		`"retry":{"max_attempts":11}`:                      "retry.max_attempts",
		`"retry":{"base_backoff":"5s","max_backoff":"1s"}`: "retry.max_backoff",
		`"hints":{"decimal_style":"fr"}`:                   "hints.decimal_style",
		`"rate_limit_rps":-1`:                              "rate_limit_rps",
	}
	for override, field := range cases {
		key := override[:strings.Index(override, ":")]
		base := minimal()
		// replace or add the key
		var doc string
		if strings.Contains(base, key+":") {
			start := strings.Index(base, key+":")
			end := strings.Index(base[start:], ",") + start
			doc = base[:start] + override + base[end:]
		} else {
			doc = strings.TrimSuffix(strings.TrimSpace(base), "}") + "," + override + "}"
		}
		_, err := Parse([]byte(doc))
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("override %s: expected error naming %q, got %v", override, field, err)
		}
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	doc := strings.TrimSuffix(strings.TrimSpace(minimal()), "}") + `,"typo":1}`
	if _, err := Parse([]byte(doc)); err == nil {
		t.Fatal("expected unknown-field error")
	}
}

func TestLoadRejectsDuplicateAndInvalid(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.json"), []byte(minimal()), 0o644)
	os.WriteFile(filepath.Join(dir, "b.json"), []byte(minimal()), 0o644)
	os.WriteFile(filepath.Join(dir, "schema.json"), []byte(`{"$schema":"x"}`), 0o644)
	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), "duplicate source_id") {
		t.Fatalf("got %v", err)
	}
	os.WriteFile(filepath.Join(dir, "b.json"), []byte(`{"source_id":"y"}`), 0o644)
	_, err = Load(dir)
	if err == nil || !strings.Contains(err.Error(), "b.json") {
		t.Fatalf("got %v", err)
	}
}
