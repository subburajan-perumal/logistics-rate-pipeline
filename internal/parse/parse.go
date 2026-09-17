// Package parse turns a streamed response body into raw records using the
// recipe's column mapping. Both parsers stream: CSV row by row, JSON element
// by element at the top-level records_path (docs/PLAN.md §8.4).
package parse

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
)

// Options carries the per-page context a parser needs to stamp records.
type Options struct {
	Recipe    recipe.Recipe
	RunID     string
	FetchedAt time.Time
	Page      int
	SourceRef string
}

// Result reports what one page yielded.
type Result struct {
	Records int
	Next    string // next page number / cursor from the body; "" when none
}

// Emit receives each record; returning an error stops parsing.
type Emit func(*record.RawRecord) error

// Parse dispatches on recipe.Format, unwrapping gzip when configured.
func Parse(ctx context.Context, body io.Reader, opts Options, emit Emit) (Result, error) {
	if opts.Recipe.Compression == "gzip" {
		gz, err := gzip.NewReader(body)
		if err != nil {
			return Result{}, fmt.Errorf("gzip: %w", err)
		}
		defer func() { _ = gz.Close() }() // gzip.Reader.Close never fails on a fully-read stream; nothing to do with it
		body = gz
	}
	switch opts.Recipe.Format {
	case "csv":
		return parseCSV(ctx, body, opts, emit)
	case "json":
		return parseJSON(ctx, body, opts, emit)
	default:
		return Result{}, fmt.Errorf("unsupported format %q", opts.Recipe.Format)
	}
}

// ParseNumber applies the recipe's decimal style. It never errors: an
// unparseable price is nil and Spark decides (D-11).
func ParseNumber(s, style string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	switch style {
	case "eu": // 1.085,00
		s = strings.ReplaceAll(s, ".", "")
		s = strings.ReplaceAll(s, ",", ".")
	case "us": // 1,085.00
		s = strings.ReplaceAll(s, ",", "")
	}
	s = strings.TrimPrefix(s, "$")
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}

// noSurcharges is a shared zero-length slice: it encodes as [] and any append
// allocates a fresh backing array, so sharing it is safe and saves one
// allocation per row.
var noSurcharges = []record.Surcharge{}

func newRecord(opts Options, row int) *record.RawRecord {
	return &record.RawRecord{
		SchemaVersion: record.SchemaVersion,
		RunID:         opts.RunID,
		SourceID:      opts.Recipe.SourceID,
		Carrier:       opts.Recipe.Carrier,
		FetchedAt:     opts.FetchedAt,
		Page:          opts.Page,
		RowIndex:      row,
		Unit:          opts.Recipe.Columns.Unit,
		SourceRef:     opts.SourceRef,
		Surcharges:    noSurcharges,
	}
}

func finish(r *record.RawRecord, opts Options) {
	r.Currency = strings.ToUpper(strings.TrimSpace(r.Currency))
	if r.Currency == "" {
		r.Currency = strings.ToUpper(opts.Recipe.Columns.CurrencyConst)
	}
	r.Price = ParseNumber(r.PriceRaw, opts.Recipe.Hints.DecimalStyle)
	for i := range r.Surcharges {
		r.Surcharges[i].Amount = ParseNumber(r.Surcharges[i].AmountRaw, opts.Recipe.Hints.DecimalStyle)
	}
	r.ComputeHash()
}

// ---- CSV ----

func parseCSV(ctx context.Context, body io.Reader, opts Options, emit Emit) (Result, error) {
	br := bufio.NewReaderSize(body, 64<<10)
	// Strip a UTF-8 BOM if present.
	if b, err := br.Peek(3); err == nil && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		_, _ = br.Discard(3)
	}
	cr := csv.NewReader(br)
	cr.ReuseRecord = true
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true

	header, err := cr.Read()
	if err != nil {
		return Result{}, fmt.Errorf("csv header: %w", err)
	}
	idx := make(map[string]int, len(header))
	for i, h := range header {
		idx[strings.ToLower(strings.TrimSpace(h))] = i
	}
	col := func(name string) (int, error) {
		if name == "" {
			return -1, nil
		}
		i, ok := idx[strings.ToLower(name)]
		if !ok {
			return -1, fmt.Errorf("csv: column %q not in header %v", name, header)
		}
		return i, nil
	}
	c := opts.Recipe.Columns
	var cols struct{ o, d, t, p, cur, vf, vt int }
	var errs []error
	for _, pair := range []struct {
		dst  *int
		name string
	}{{&cols.o, c.Origin}, {&cols.d, c.Destination}, {&cols.t, c.ContainerType}, {&cols.p, c.Price},
		{&cols.cur, c.Currency}, {&cols.vf, c.ValidFrom}, {&cols.vt, c.ValidTo}} {
		i, err := col(pair.name)
		if err != nil {
			errs = append(errs, err)
		}
		*pair.dst = i
	}
	surIdx := make([]int, len(c.Surcharges))
	for i, s := range c.Surcharges {
		j, err := col(s.Column)
		if err != nil {
			errs = append(errs, err)
		}
		surIdx[i] = j
	}
	if err := errors.Join(errs...); err != nil {
		return Result{}, err
	}
	get := func(rec []string, i int) string {
		if i < 0 || i >= len(rec) {
			return ""
		}
		return rec[i]
	}

	n := 0
	for {
		if err := ctx.Err(); err != nil {
			return Result{Records: n}, err
		}
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Result{Records: n}, fmt.Errorf("csv row %d: %w", n+1, err)
		}
		r := newRecord(opts, n)
		r.OriginRaw = get(rec, cols.o)
		r.DestinationRaw = get(rec, cols.d)
		r.ContainerTypeRaw = get(rec, cols.t)
		r.PriceRaw = get(rec, cols.p)
		r.Currency = get(rec, cols.cur)
		r.ValidFromRaw = get(rec, cols.vf)
		r.ValidToRaw = get(rec, cols.vt)
		for i, s := range c.Surcharges {
			r.Surcharges = append(r.Surcharges, record.Surcharge{Name: s.Name, AmountRaw: get(rec, surIdx[i])})
		}
		finish(r, opts)
		if err := emit(r); err != nil {
			return Result{Records: n}, err
		}
		n++
	}
	return Result{Records: n}, nil
}

// ---- JSON ----

// parseJSON walks the top-level object with the token API, streaming the
// array at records_path element by element and capturing pagination.path.
func parseJSON(ctx context.Context, body io.Reader, opts Options, emit Emit) (Result, error) {
	dec := json.NewDecoder(bufio.NewReaderSize(body, 64<<10))
	dec.UseNumber()
	res := Result{}

	tok, err := dec.Token()
	if err != nil {
		return res, fmt.Errorf("json: %w", err)
	}
	// Top level may itself be the array.
	if d, ok := tok.(json.Delim); ok && d == '[' {
		if opts.Recipe.RecordsPath != "." {
			return res, fmt.Errorf("json: top-level array but records_path is %q (use \".\")", opts.Recipe.RecordsPath)
		}
		n, err := streamArray(ctx, dec, opts, emit, 0)
		res.Records = n
		return res, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return res, fmt.Errorf("json: expected object or array, got %v", tok)
	}
	nextKey := opts.Recipe.Pagination.Path
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return res, fmt.Errorf("json: %w", err)
		}
		key, _ := kt.(string)
		switch {
		case key == opts.Recipe.RecordsPath:
			t, err := dec.Token()
			if err != nil {
				return res, err
			}
			if d, ok := t.(json.Delim); !ok || d != '[' {
				return res, fmt.Errorf("json: records_path %q is not an array", key)
			}
			n, err := streamArray(ctx, dec, opts, emit, res.Records)
			res.Records += n
			if err != nil {
				return res, err
			}
		case nextKey != "" && key == nextKey:
			var v any
			if err := dec.Decode(&v); err != nil {
				return res, err
			}
			switch x := v.(type) {
			case string:
				res.Next = x
			case json.Number:
				res.Next = x.String()
			case nil:
				res.Next = ""
			default:
				return res, fmt.Errorf("json: %s must be string, number or null", nextKey)
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

// streamArray decodes elements one at a time until the closing bracket.
func streamArray(ctx context.Context, dec *json.Decoder, opts Options, emit Emit, start int) (int, error) {
	n := 0
	for dec.More() {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		var el map[string]any
		if err := dec.Decode(&el); err != nil {
			return n, fmt.Errorf("json element %d: %w", start+n, err)
		}
		k, err := mapElement(el, opts, start+n, emit)
		if err != nil {
			return n, err
		}
		n += k
	}
	if _, err := dec.Token(); err != nil { // consume ']'
		return n, err
	}
	return n, nil
}

// lookup resolves a dotted path in a decoded JSON object.
func lookup(el map[string]any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}
	var cur any = el
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func str(v any, ok bool) string {
	if !ok || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprint(x)
	}
}

// mapElement produces one record (or one per exploded key) from an element.
func mapElement(el map[string]any, opts Options, row int, emit Emit) (int, error) {
	c := opts.Recipe.Columns
	base := func(r *record.RawRecord) {
		r.OriginRaw = str(lookup(el, c.Origin))
		r.DestinationRaw = str(lookup(el, c.Destination))
		r.Currency = str(lookup(el, c.Currency))
		r.ValidFromRaw = str(lookup(el, c.ValidFrom))
		r.ValidToRaw = str(lookup(el, c.ValidTo))
		for _, s := range c.Surcharges {
			r.Surcharges = append(r.Surcharges, record.Surcharge{Name: s.Name, AmountRaw: str(lookup(el, s.Column))})
		}
	}
	if opts.Recipe.Explode == nil {
		r := newRecord(opts, row)
		base(r)
		r.ContainerTypeRaw = str(lookup(el, c.ContainerType))
		r.PriceRaw = str(lookup(el, c.Price))
		finish(r, opts)
		return 1, emit(r)
	}
	v, ok := lookup(el, opts.Recipe.Explode.Path)
	m, isMap := v.(map[string]any)
	if !ok || !isMap {
		return 0, fmt.Errorf("json element %d: explode path %q missing or not an object", row, opts.Recipe.Explode.Path)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	n := 0
	for _, k := range keys {
		r := newRecord(opts, row)
		base(r)
		r.ContainerTypeRaw = k
		r.PriceRaw = str(m[k], true)
		finish(r, opts)
		if err := emit(r); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
