// Package recipe loads and validates the declarative source recipes
// (docs/PLAN.md §6.3). A recipe is the only contract between a feed and
// ingestd: adding a source is adding a file, not code (D-10).
package recipe

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Duration is a time.Duration that (un)marshals as a Go duration string.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"20s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// Auth describes how a request is authenticated.
type Auth struct {
	Type      string `json:"type"`                 // none | api_key
	Header    string `json:"header,omitempty"`     // api_key: header name
	SecretEnv string `json:"secret_env,omitempty"` // api_key: env var holding the value
}

// Pagination describes how to walk a multi-page feed.
type Pagination struct {
	Type     string `json:"type"`                // none | page | cursor
	Param    string `json:"param,omitempty"`     // query parameter carrying the page number / cursor
	Start    int    `json:"start,omitempty"`     // page: first page number
	Path     string `json:"path,omitempty"`      // json top-level key holding next page/cursor
	MaxPages int    `json:"max_pages,omitempty"` // safety bound (default 100)
}

// SurchargeColumn maps a named surcharge to a source column/path.
type SurchargeColumn struct {
	Name   string `json:"name"`
	Column string `json:"column"`
}

// Columns maps target raw-record fields to source columns (csv) or dotted
// paths (json). Unit is a literal, not a column.
type Columns struct {
	Origin        string            `json:"origin_raw"`
	Destination   string            `json:"destination_raw"`
	ContainerType string            `json:"container_type_raw"`
	Price         string            `json:"price_raw"`
	Currency      string            `json:"currency"`
	CurrencyConst string            `json:"currency_const,omitempty"`
	Unit          string            `json:"unit"`
	ValidFrom     string            `json:"valid_from_raw"`
	ValidTo       string            `json:"valid_to_raw"`
	Surcharges    []SurchargeColumn `json:"surcharges,omitempty"`
}

// Explode turns one source object with a map of container→price into one
// record per map entry (the nested "aurora" shape).
type Explode struct {
	Path string `json:"path"` // dotted path to the {container_type: price} object; key → container_type_raw, value → price_raw
}

// Hints control light typing done in Go.
type Hints struct {
	DecimalStyle string `json:"decimal_style,omitempty"` // us | eu | plain
	DateLayout   string `json:"date_layout,omitempty"`   // documentation only; Spark parses dates
}

// Retry is the backoff policy (D-13).
type Retry struct {
	MaxAttempts int      `json:"max_attempts"`
	BaseBackoff Duration `json:"base_backoff"`
	MaxBackoff  Duration `json:"max_backoff"`
}

// Recipe is one source.
type Recipe struct {
	SourceID     string     `json:"source_id"`
	Carrier      string     `json:"carrier"`
	URL          string     `json:"url"`
	Format       string     `json:"format"`      // csv | json
	Compression  string     `json:"compression"` // none | gzip
	Auth         Auth       `json:"auth"`
	Pagination   Pagination `json:"pagination"`
	RecordsPath  string     `json:"records_path,omitempty"`
	Explode      *Explode   `json:"explode,omitempty"`
	Columns      Columns    `json:"columns"`
	Hints        Hints      `json:"hints"`
	Timeout      Duration   `json:"timeout"`
	Retry        Retry      `json:"retry"`
	RateLimitRPS float64    `json:"rate_limit_rps"`
}

var idRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// Validate applies the constraints documented in recipes/schema.json and
// fills defaults. Errors name the offending field.
func (r *Recipe) Validate() error {
	var errs []error
	fail := func(f, msg string) { errs = append(errs, fmt.Errorf("%s: %s", f, msg)) }

	if !idRe.MatchString(r.SourceID) {
		fail("source_id", "must match ^[a-z][a-z0-9_]{0,31}$")
	}
	if r.Carrier == "" || strings.ToUpper(r.Carrier) != r.Carrier {
		fail("carrier", "required, upper-case")
	}
	if !strings.HasPrefix(r.URL, "http://") && !strings.HasPrefix(r.URL, "https://") {
		fail("url", "must be http(s)")
	}
	switch r.Format {
	case "csv", "json":
	default:
		fail("format", "must be csv or json")
	}
	switch r.Compression {
	case "":
		r.Compression = "none"
	case "none", "gzip":
	default:
		fail("compression", "must be none or gzip")
	}
	switch r.Auth.Type {
	case "":
		r.Auth.Type = "none"
	case "none":
	case "api_key":
		if r.Auth.Header == "" || r.Auth.SecretEnv == "" {
			fail("auth", "api_key needs header and secret_env")
		}
	default:
		fail("auth.type", "must be none or api_key")
	}
	switch r.Pagination.Type {
	case "":
		r.Pagination.Type = "none"
	case "none":
	case "page", "cursor":
		if r.Pagination.Param == "" || r.Pagination.Path == "" {
			fail("pagination", "page/cursor need param and path")
		}
		if r.Format != "json" {
			fail("pagination", "only json feeds paginate")
		}
		if r.Pagination.MaxPages == 0 {
			r.Pagination.MaxPages = 100
		}
		if r.Pagination.Type == "page" && r.Pagination.Start == 0 {
			r.Pagination.Start = 1
		}
	default:
		fail("pagination.type", "must be none, page or cursor")
	}
	if r.Format == "json" && r.RecordsPath == "" {
		fail("records_path", "required for json feeds")
	}
	if r.Explode != nil && r.Explode.Path == "" {
		fail("explode", "needs path")
	}
	c := r.Columns
	if c.Origin == "" || c.Destination == "" {
		fail("columns", "origin_raw and destination_raw are required")
	}
	if r.Explode == nil && (c.ContainerType == "" || c.Price == "") {
		fail("columns", "container_type_raw and price_raw are required unless explode is set")
	}
	if c.Currency == "" && c.CurrencyConst == "" {
		fail("columns", "currency or currency_const is required")
	}
	switch c.Unit {
	case "per_container", "per_teu":
	default:
		fail("columns.unit", "must be per_container or per_teu")
	}
	switch r.Hints.DecimalStyle {
	case "":
		r.Hints.DecimalStyle = "us"
	case "us", "eu", "plain":
	default:
		fail("hints.decimal_style", "must be us, eu or plain")
	}
	if r.Timeout.Duration == 0 {
		r.Timeout.Duration = 20 * time.Second
	}
	if r.Retry.MaxAttempts == 0 {
		r.Retry.MaxAttempts = 4
	}
	if r.Retry.MaxAttempts < 1 || r.Retry.MaxAttempts > 10 {
		fail("retry.max_attempts", "must be 1..10")
	}
	if r.Retry.BaseBackoff.Duration == 0 {
		r.Retry.BaseBackoff.Duration = 250 * time.Millisecond
	}
	if r.Retry.MaxBackoff.Duration == 0 {
		r.Retry.MaxBackoff.Duration = 5 * time.Second
	}
	if r.Retry.MaxBackoff.Duration < r.Retry.BaseBackoff.Duration {
		fail("retry.max_backoff", "must be >= base_backoff")
	}
	if r.RateLimitRPS < 0 {
		fail("rate_limit_rps", "must be >= 0")
	}
	return errors.Join(errs...)
}

// Parse decodes one recipe from JSON, expanding ${ENV} in the url, and validates it.
func Parse(b []byte) (Recipe, error) {
	var r Recipe
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Errorf("decode: %w", err)
	}
	r.URL = os.Expand(r.URL, func(k string) string { return os.Getenv(k) })
	if err := r.Validate(); err != nil {
		return r, err
	}
	return r, nil
}

// Set is a validated, ordered collection of recipes plus a content hash.
type Set struct {
	Recipes []Recipe
	SHA256  string
}

// Load reads every *.json in dir except schema.json. Any invalid file fails
// the whole load — misconfiguration must be visible at startup, not on the
// first run.
func Load(dir string) (*Set, error) {
	entries, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(entries)
	h := sha256.New()
	set := &Set{}
	seen := map[string]string{}
	var errs []error
	for _, p := range entries {
		if filepath.Base(p) == "schema.json" {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		h.Write(b)
		r, err := Parse(b)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", filepath.Base(p), err))
			continue
		}
		if prev, dup := seen[r.SourceID]; dup {
			errs = append(errs, fmt.Errorf("%s: duplicate source_id %q (also in %s)", filepath.Base(p), r.SourceID, prev))
			continue
		}
		seen[r.SourceID] = filepath.Base(p)
		set.Recipes = append(set.Recipes, r)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if len(set.Recipes) == 0 {
		return nil, fmt.Errorf("no recipes found in %s", dir)
	}
	set.SHA256 = hex.EncodeToString(h.Sum(nil))
	return set, nil
}
