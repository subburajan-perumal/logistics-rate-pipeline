// Package mock serves the eight synthetic carrier feeds (docs/PLAN.md §6.2).
// Everything is derived from a seed and an as-of date: the same request
// sequence yields the same bytes, the same latencies and the same injected
// failures (D-07, D-09). Nothing here is real carrier data.
package mock

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Config controls the server.
type Config struct {
	Seed         uint64
	AsOf         string  // documentation; validity windows are fixed below
	LatencyScale float64 // 1.0 = documented latencies; 0 = none
	Failures     bool    // inject delphine's 500s/429
	APIKey       string  // eventide's expected X-Api-Key
}

// DefaultSeed matches the plan.
const DefaultSeed = 20260917

// Validity windows.
const (
	ValidFrom      = "2026-07-01"
	ValidTo        = "2026-12-31"
	SupersededFrom = "2026-04-01"
	SupersededTo   = "2026-06-30"
)

// Row is one lane × type rate in canonical form before each feed's quirks
// are applied.
type Row struct {
	Origin, Destination Port
	Type                string // 20DRY | 40DRY | 40HC
	Price               float64
	Currency            string
	ValidFrom, ValidTo  string
}

// fx is the inverse of spark/config/fx_rates.csv (units of currency per USD).
var fx = map[string]float64{"USD": 1, "EUR": 1 / 1.08, "INR": 1 / 0.012, "GBP": 1 / 1.27, "SGD": 1 / 0.74, "AED": 1 / 0.2723}

// carrierSpec fixes each carrier's lane count, currency and price multiplier.
type carrierSpec struct {
	id, carrier string
	lanes       int
	currency    string
	mult        float64
}

var specs = []carrierSpec{
	{"meridian", "MERIDIAN", 58, "USD", 1.00},
	{"halcyon", "HALCYON", 40, "EUR", 0.92},
	{"aurora", "AURORA", 50, "INR", 1.05},
	{"borealis", "BOREALIS", 30, "GBP", 0.97},
	{"corvus", "CORVUS", 45, "USD", 0.88},
	{"delphine", "DELPHINE", 35, "USD", 1.10},
	{"eventide", "EVENTIDE", 25, "SGD", 1.02},
	{"fathom", "FATHOM", 500, "USD", 0.95},
}

var types = []string{"20DRY", "40DRY", "40HC"}

// Gen produces deterministic rows.
type Gen struct{ seed uint64 }

func NewGen(seed uint64) *Gen { return &Gen{seed: seed} }

func (g *Gen) rng(parts ...string) *rand.Rand {
	h := fnv.New64a()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return rand.New(rand.NewPCG(g.seed, h.Sum64()))
}

// Lanes returns the carrier's deterministic lane subset from the 500 pairs.
func (g *Gen) Lanes(spec carrierSpec) [][2]Port {
	all := make([][2]Port, 0, len(Origins)*len(Destinations))
	for _, o := range Origins {
		for _, d := range Destinations {
			all = append(all, [2]Port{o, d})
		}
	}
	r := g.rng("lanes", spec.id)
	r.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	return all[:spec.lanes]
}

// BasePriceUSD is stable per (lane, type) across carriers so overlapping
// lanes look related, then each carrier applies its multiplier.
func (g *Gen) BasePriceUSD(o, d Port, typ string) float64 {
	r := g.rng("price", o.Locode, d.Locode)
	p20 := 900 + r.IntN(1001)
	p40 := 1700 + r.IntN(1601)
	phc := p40 + 90 + r.IntN(171)
	switch typ {
	case "20DRY":
		return float64(p20)
	case "40DRY":
		return float64(p40)
	default:
		return float64(phc)
	}
}

// Rows generates a carrier's current rows in a fixed order.
func (g *Gen) Rows(spec carrierSpec) []Row {
	var rows []Row
	for _, l := range g.Lanes(spec) {
		for _, t := range types {
			usd := g.BasePriceUSD(l[0], l[1], t) * spec.mult
			rows = append(rows, Row{
				Origin: l[0], Destination: l[1], Type: t,
				Price: roundTo(usd*fx[spec.currency], 2), Currency: spec.currency,
				ValidFrom: ValidFrom, ValidTo: ValidTo,
			})
		}
	}
	return rows
}

func roundTo(v float64, dp int) float64 {
	m := 1.0
	for i := 0; i < dp; i++ {
		m *= 10
	}
	return float64(int64(v*m+0.5)) / m
}

// ---- server ----

// Server is the http.Handler serving all eight feeds.
type Server struct {
	cfg  Config
	gen  *Gen
	reqs map[string]*atomic.Int64 // per-source request counter (failure schedule)
}

// NewServer builds the handler.
func NewServer(cfg Config) *Server {
	if cfg.Seed == 0 {
		cfg.Seed = DefaultSeed
	}
	if cfg.APIKey == "" {
		cfg.APIKey = "eventide-demo-key"
	}
	s := &Server{cfg: cfg, gen: NewGen(cfg.Seed), reqs: map[string]*atomic.Int64{}}
	for _, sp := range specs {
		s.reqs[sp.id] = &atomic.Int64{}
	}
	return s
}

// Handler routes /<source>/... paths.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /meridian/rates", s.meridian)
	mux.HandleFunc("GET /halcyon/tariff.csv", s.halcyon)
	mux.HandleFunc("GET /aurora/v2/lanes", s.aurora)
	mux.HandleFunc("GET /borealis/rates.json", s.borealis)
	mux.HandleFunc("GET /corvus/export.csv", s.corvus)
	mux.HandleFunc("GET /delphine/rates", s.delphine)
	mux.HandleFunc("GET /eventide/rates", s.eventide)
	mux.HandleFunc("GET /fathom/bulk.csv.gz", s.fathom)
	return mux
}

func (s *Server) sleep(d time.Duration) {
	if s.cfg.LatencyScale > 0 {
		time.Sleep(time.Duration(float64(d) * s.cfg.LatencyScale))
	}
}

func (s *Server) count(id string) int64 { return s.reqs[id].Add(1) }

func spec(id string) carrierSpec {
	for _, sp := range specs {
		if sp.id == id {
			return sp
		}
	}
	panic("unknown source " + id)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func pageOf[T any](items []T, page, size int) ([]T, bool) {
	start := (page - 1) * size
	if start >= len(items) || page < 1 {
		return nil, false
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], end < len(items)
}

// 1. meridian — paginated JSON, LOCODEs, USD, ISO dates; 6 superseded rows (2 lanes).
func (s *Server) meridian(w http.ResponseWriter, r *http.Request) {
	s.count("meridian")
	s.sleep(150 * time.Millisecond)
	sp := spec("meridian")
	rows := s.gen.Rows(sp)
	lanes := s.gen.Lanes(sp)
	for _, l := range lanes[:2] { // superseded Q2 rows for the first two lanes
		for _, t := range types {
			usd := s.gen.BasePriceUSD(l[0], l[1], t) * sp.mult * 0.93
			rows = append(rows, Row{Origin: l[0], Destination: l[1], Type: t, Price: roundTo(usd, 0),
				Currency: "USD", ValidFrom: SupersededFrom, ValidTo: SupersededTo})
		}
	}
	type item struct {
		OriginLocode string  `json:"origin_locode"`
		DestLocode   string  `json:"destination_locode"`
		Container    string  `json:"container_type"`
		Rate         float64 `json:"rate"`
		Currency     string  `json:"currency"`
		ValidFrom    string  `json:"valid_from"`
		ValidTo      string  `json:"valid_to"`
		TariffRef    string  `json:"tariff_ref"`
	}
	items := make([]item, 0, len(rows))
	for _, x := range rows {
		ref := "MER-2026-H2"
		if x.ValidTo == SupersededTo {
			ref = "MER-2026-Q2"
		}
		items = append(items, item{x.Origin.Locode, x.Destination.Locode, x.Type, x.Price, x.Currency, x.ValidFrom, x.ValidTo, ref})
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page == 0 {
		page = 1
	}
	chunk, more := pageOf(items, page, 50)
	var next any
	if more {
		next = page + 1
	}
	writeJSON(w, map[string]any{"carrier": "MERIDIAN", "page": page, "items": chunk, "next_page": next})
}

// 2. halcyon — CSV, EUR, BAF column, EU decimals, dd/mm/yyyy, equipment aliases; 2 reversed-date rows.
func (s *Server) halcyon(w http.ResponseWriter, r *http.Request) {
	s.count("halcyon")
	s.sleep(300 * time.Millisecond)
	rows := s.gen.Rows(spec("halcyon"))
	alias := map[string]string{"20DRY": "20GP", "40DRY": "40GP", "40HC": "40HQ"}
	w.Header().Set("Content-Type", "text/csv")
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"Origin", "Destination", "Equipment", "Rate", "Currency", "BAF", "ValidFrom", "ValidTo"})
	for i, x := range rows {
		vf, vt := euDate(x.ValidFrom), euDate(x.ValidTo)
		if i == 0 || i == 3 { // first 20GP row of lanes 0 and 1: dates reversed
			vf, vt = vt, vf
		}
		baf := "120,00"
		if x.Type != "20DRY" {
			baf = "240,00"
		}
		_ = cw.Write([]string{x.Origin.Locode, x.Destination.Locode, alias[x.Type], euNumber(x.Price), "EUR", baf, vf, vt})
	}
	cw.Flush()
}

func euDate(iso string) string { return iso[8:10] + "/" + iso[5:7] + "/" + iso[0:4] }

func euNumber(v float64) string {
	s := strconv.FormatFloat(v, 'f', 2, 64) // 1085.00
	i := strings.IndexByte(s, '.')
	whole, frac := s[:i], s[i+1:]
	var b strings.Builder
	for j, c := range whole {
		if j > 0 && (len(whole)-j)%3 == 0 {
			b.WriteByte('.')
		}
		b.WriteRune(c)
	}
	return b.String() + "," + frac
}

// 3. aurora — nested JSON, cursor pagination, INR, slow.
func (s *Server) aurora(w http.ResponseWriter, r *http.Request) {
	s.count("aurora")
	cursor := r.URL.Query().Get("cursor")
	page := 1
	if strings.HasPrefix(cursor, "p") {
		page, _ = strconv.Atoi(cursor[1:])
	}
	lat := 1500 + s.gen.rng("aurora-latency", strconv.Itoa(page)).IntN(1001) // 1.5–2.5 s, fixed per page
	s.sleep(time.Duration(lat) * time.Millisecond)
	sp := spec("aurora")
	lanes := s.gen.Lanes(sp)
	type lane struct {
		Origin      string             `json:"origin"`
		Destination string             `json:"destination"`
		Currency    string             `json:"currency"`
		Valid       map[string]string  `json:"valid"`
		Containers  map[string]float64 `json:"containers"`
	}
	all := make([]lane, 0, len(lanes))
	for _, l := range lanes {
		c := map[string]float64{}
		for _, t := range types {
			c[t] = roundTo(s.gen.BasePriceUSD(l[0], l[1], t)*sp.mult*fx["INR"], 0)
		}
		all = append(all, lane{l[0].Locode, l[1].Locode, "INR", map[string]string{"from": ValidFrom, "to": ValidTo}, c})
	}
	chunk, more := pageOf(all, page, 25)
	var next any
	if more {
		next = "p" + strconv.Itoa(page+1)
	}
	writeJSON(w, map[string]any{"lanes": chunk, "next_cursor": next})
}

// 4. borealis — top-level JSON array, city names/aliases, GBP; one lane from "Springfield".
func (s *Server) borealis(w http.ResponseWriter, r *http.Request) {
	s.count("borealis")
	s.sleep(200 * time.Millisecond)
	rows := s.gen.Rows(spec("borealis"))
	rng := s.gen.rng("borealis-names")
	name := func(p Port) string {
		if len(p.Aliases) > 0 && rng.IntN(3) == 0 {
			return p.Aliases[0]
		}
		if rng.IntN(4) == 0 {
			return strings.ToUpper(p.City)
		}
		return p.City
	}
	out := make([]map[string]any, 0, len(rows))
	for i, x := range rows {
		o := name(x.Origin)
		if i < 3 {
			o = "Springfield"
		}
		out = append(out, map[string]any{
			"origin_city": o, "destination_city": name(x.Destination), "container": x.Type,
			"price_gbp": x.Price, "currency": "GBP", "valid_from": x.ValidFrom, "valid_to": x.ValidTo,
		})
	}
	writeJSON(w, out)
}

// 5. corvus — CSV, per-TEU pricing with sizes 20/40; one negative and one blank price.
func (s *Server) corvus(w http.ResponseWriter, r *http.Request) {
	s.count("corvus")
	s.sleep(200 * time.Millisecond)
	sp := spec("corvus")
	w.Header().Set("Content-Type", "text/csv")
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"origin", "destination", "size", "teu_rate", "currency", "valid_from", "valid_to"})
	i := 0
	for _, l := range s.gen.Lanes(sp) {
		for _, size := range []string{"20", "40"} {
			t := "20DRY"
			if size == "40" {
				t = "40DRY"
			}
			usd := s.gen.BasePriceUSD(l[0], l[1], t) * sp.mult
			if size == "40" {
				usd /= 2 // quoted per TEU
			}
			price := strconv.FormatFloat(roundTo(usd, 2), 'f', 2, 64)
			switch i {
			case 0:
				price = "-120"
			case 2:
				price = ""
			}
			_ = cw.Write([]string{l[0].Locode, l[1].Locode, size, price, "USD", ValidFrom, ValidTo})
			i++
		}
	}
	cw.Flush()
}

// 6. delphine — paginated JSON with injected 500s and one 429; two 45HC rows.
func (s *Server) delphine(w http.ResponseWriter, r *http.Request) {
	n := s.count("delphine")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page == 0 {
		page = 1
	}
	s.sleep(100 * time.Millisecond)
	if s.cfg.Failures {
		// Failure schedule keyed by the per-source request sequence, so it is
		// deterministic for a server lifetime and every run sees both kinds.
		switch {
		case n%3 == 0:
			http.Error(w, "upstream flake", http.StatusInternalServerError)
			return
		case n%5 == 2:
			w.Header().Set("Retry-After", "2")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
	}
	rows := s.gen.Rows(spec("delphine"))
	items := make([]map[string]any, 0, len(rows))
	for i, x := range rows {
		t := x.Type
		if (i == 2 || i == 5) && t == "40HC" { // lanes 0 and 1 quote 45HC instead of 40HC
			t = "45HC"
		}
		items = append(items, map[string]any{"origin": x.Origin.Locode, "destination": x.Destination.Locode,
			"equipment": t, "amount": x.Price, "ccy": "USD", "valid_from": x.ValidFrom, "valid_to": x.ValidTo})
	}
	chunk, more := pageOf(items, page, 50)
	var next any
	if more {
		next = page + 1
	}
	writeJSON(w, map[string]any{"items": chunk, "next_page": next})
}

// 7. eventide — requires X-Api-Key; SGD/AED alternating; one "EURO" row.
func (s *Server) eventide(w http.ResponseWriter, r *http.Request) {
	s.count("eventide")
	s.sleep(100 * time.Millisecond)
	if r.Header.Get("X-Api-Key") != s.cfg.APIKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	sp := spec("eventide")
	items := make([]map[string]any, 0)
	for li, l := range s.gen.Lanes(sp) {
		ccy := "SGD"
		if li%2 == 1 {
			ccy = "AED"
		}
		for ti, t := range types {
			c := ccy
			if li == 0 && ti == 0 {
				c = "EURO"
			}
			items = append(items, map[string]any{"origin": l[0].Locode, "destination": l[1].Locode, "type": t,
				"rate": roundTo(s.gen.BasePriceUSD(l[0], l[1], t)*sp.mult*fx[ccy], 2), "currency": c,
				"valid_from": ValidFrom, "valid_to": ValidTo})
		}
	}
	writeJSON(w, map[string]any{"data": items})
}

// FathomRows is the number of rows the bulk feed serves.
const FathomRows = 20000

// 8. fathom — gzip CSV bulk: 1500 distinct lane rows + 50 same-port rows +
// 18,450 duplicates (exact or case/whitespace variants), shuffled.
func (s *Server) fathom(w http.ResponseWriter, r *http.Request) {
	s.count("fathom")
	s.sleep(800 * time.Millisecond)
	sp := spec("fathom")
	base := s.gen.Rows(sp) // 1500
	type rec [7]string
	recs := make([]rec, 0, FathomRows)
	for _, x := range base {
		recs = append(recs, rec{x.Origin.Locode, x.Destination.Locode, x.Type, strconv.FormatFloat(x.Price, 'f', 2, 64), "USD", x.ValidFrom, x.ValidTo})
	}
	for i := 0; i < 50; i++ { // same_port rows
		o := Origins[i%len(Origins)]
		recs = append(recs, rec{o.Locode, o.Locode, types[i%3], "1000.00", "USD", ValidFrom, ValidTo})
	}
	rng := s.gen.rng("fathom-dups")
	for len(recs) < FathomRows { // duplicates of non-trap rows
		src := recs[rng.IntN(len(base))]
		switch rng.IntN(3) {
		case 1:
			src[0] = strings.ToLower(src[0])
		case 2:
			src[1] = " " + src[1] + " "
		}
		recs = append(recs, src)
	}
	rng.Shuffle(len(recs), func(i, j int) { recs[i], recs[j] = recs[j], recs[i] })

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	cw := csv.NewWriter(gz)
	_ = cw.Write([]string{"origin", "destination", "type", "price", "currency", "valid_from", "valid_to"})
	for _, x := range recs {
		_ = cw.Write(x[:])
	}
	cw.Flush()
	_ = gz.Close()
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	_, _ = w.Write(buf.Bytes())
}

// ExpectedRaw is the raw row count a healthy run lands, per source.
var ExpectedRaw = map[string]int{
	"meridian": 180, "halcyon": 120, "aurora": 150, "borealis": 90,
	"corvus": 90, "delphine": 105, "eventide": 75, "fathom": FathomRows,
}

// ExpectedTotal sums ExpectedRaw.
func ExpectedTotal() int {
	n := 0
	for _, v := range ExpectedRaw {
		n += v
	}
	return n
}

// String renders the spec table for docs/sources.md.
func (s *Server) String() string {
	return fmt.Sprintf("mocksources seed=%d failures=%v latency=%.2f", s.cfg.Seed, s.cfg.Failures, s.cfg.LatencyScale)
}
