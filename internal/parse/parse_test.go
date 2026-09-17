package parse

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/recipe"
	"github.com/subburajan-perumal/logistics-rate-pipeline/internal/record"
)

func csvRecipe(t *testing.T) recipe.Recipe {
	t.Helper()
	r := recipe.Recipe{SourceID: "t", Carrier: "T", URL: "http://x", Format: "csv",
		Columns: recipe.Columns{Origin: "Origin", Destination: "Destination", ContainerType: "Equipment",
			Price: "Rate", Currency: "Currency", Unit: "per_container", ValidFrom: "ValidFrom", ValidTo: "ValidTo",
			Surcharges: []recipe.SurchargeColumn{{Name: "BAF", Column: "BAF"}}},
		Hints: recipe.Hints{DecimalStyle: "eu"}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func collect(t *testing.T, body string, rc recipe.Recipe) ([]*record.RawRecord, Result) {
	t.Helper()
	var out []*record.RawRecord
	res, err := Parse(context.Background(), strings.NewReader(body), Options{Recipe: rc, RunID: "r", FetchedAt: time.Now()},
		func(r *record.RawRecord) error { out = append(out, r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	return out, res
}

func TestCSVQuotesCRLFAndBOM(t *testing.T) {
	body := "\xEF\xBB\xBFOrigin,Destination,Equipment,Rate,Currency,BAF,ValidFrom,ValidTo\r\n" +
		"INMAA,NLRTM,20GP,\"1.085,50\",EUR,\"120,00\",01/07/2026,31/12/2026\r\n" +
		"INMAA, DEHAM ,40HQ,\"2.310,00\",eur,\"240,00\",01/07/2026,31/12/2026\r\n"
	recs, res := collect(t, body, csvRecipe(t))
	if res.Records != 2 || len(recs) != 2 {
		t.Fatalf("records=%d", res.Records)
	}
	if recs[0].OriginRaw != "INMAA" || recs[0].PriceRaw != "1.085,50" || *recs[0].Price != 1085.5 {
		t.Fatalf("row0 = %+v", recs[0])
	}
	if recs[0].Surcharges[0].Amount == nil || *recs[0].Surcharges[0].Amount != 120 {
		t.Fatalf("surcharge = %+v", recs[0].Surcharges)
	}
	if recs[1].Currency != "EUR" {
		t.Fatalf("currency must be upper-cased, got %q", recs[1].Currency)
	}
	if recs[1].DestinationRaw != "DEHAM " { // csv trims leading space only; trailing kept as received
		t.Fatalf("dest = %q", recs[1].DestinationRaw)
	}
}

func TestCSVMissingColumnIsAnError(t *testing.T) {
	rc := csvRecipe(t)
	_, err := Parse(context.Background(), strings.NewReader("Origin,Destination\nA,B\n"), Options{Recipe: rc},
		func(*record.RawRecord) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "Equipment") {
		t.Fatalf("expected missing-column error, got %v", err)
	}
}

func TestUnparseablePriceIsNullNotError(t *testing.T) {
	body := "Origin,Destination,Equipment,Rate,Currency,BAF,ValidFrom,ValidTo\nA,B,20GP,n/a,EUR,,x,y\nA,B,40GP,,EUR,,x,y\n"
	recs, _ := collect(t, body, csvRecipe(t))
	if recs[0].Price != nil || recs[1].Price != nil {
		t.Fatal("expected nil prices")
	}
	if recs[0].PriceRaw != "n/a" {
		t.Fatal("raw must be preserved")
	}
}

func TestParseNumberStyles(t *testing.T) {
	cases := []struct {
		in, style string
		want      float64
	}{
		{"1,085.50", "us", 1085.5}, {"1085.5", "us", 1085.5}, {"$2,310", "us", 2310},
		{"1.085,50", "eu", 1085.5}, {"120,00", "eu", 120},
		{"139125", "plain", 139125}, {"2132.75", "plain", 2132.75},
	}
	for _, c := range cases {
		got := ParseNumber(c.in, c.style)
		if got == nil || *got != c.want {
			t.Errorf("ParseNumber(%q,%s) = %v, want %v", c.in, c.style, got, c.want)
		}
	}
	if ParseNumber("", "us") != nil || ParseNumber("abc", "eu") != nil {
		t.Error("expected nil")
	}
}

func TestGzipCSVStreams(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte("Origin,Destination,Equipment,Rate,Currency,BAF,ValidFrom,ValidTo\n"))
	for i := 0; i < 5000; i++ {
		gz.Write([]byte("A,B,20GP,\"1.000,00\",EUR,\"1,00\",x,y\n"))
	}
	gz.Close()
	rc := csvRecipe(t)
	rc.Compression = "gzip"
	n := 0
	res, err := Parse(context.Background(), &buf, Options{Recipe: rc}, func(*record.RawRecord) error { n++; return nil })
	if err != nil || res.Records != 5000 || n != 5000 {
		t.Fatalf("n=%d res=%+v err=%v", n, res, err)
	}
}

func jsonRecipe(t *testing.T, path string, pag recipe.Pagination, explode *recipe.Explode) recipe.Recipe {
	t.Helper()
	r := recipe.Recipe{SourceID: "j", Carrier: "J", URL: "http://x", Format: "json", RecordsPath: path, Pagination: pag, Explode: explode,
		Columns: recipe.Columns{Origin: "o", Destination: "d", ContainerType: "t", Price: "p", Currency: "c", Unit: "per_container",
			ValidFrom: "v.from", ValidTo: "v.to"}, Hints: recipe.Hints{DecimalStyle: "plain"}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestJSONRecordsPathAndNext(t *testing.T) {
	rc := jsonRecipe(t, "items", recipe.Pagination{Type: "page", Param: "page", Path: "next_page"}, nil)
	body := `{"meta":{"x":[1,2,{"y":null}]},"items":[{"o":"A","d":"B","t":"20DRY","p":1234,"c":"USD","v":{"from":"f","to":"t"}},{"o":"C","d":"D","t":"40HC","p":"2,310","c":"usd","v":{}}],"next_page":2}`
	recs, res := collect(t, body, rc)
	if res.Records != 2 || res.Next != "2" {
		t.Fatalf("res=%+v", res)
	}
	if recs[0].PriceRaw != "1234" || *recs[0].Price != 1234 || recs[0].ValidFromRaw != "f" {
		t.Fatalf("rec0=%+v", recs[0])
	}
	if recs[1].Currency != "USD" || recs[1].ValidToRaw != "" {
		t.Fatalf("rec1=%+v", recs[1])
	}
}

func TestJSONNextNullAndCursorString(t *testing.T) {
	rc := jsonRecipe(t, "items", recipe.Pagination{Type: "cursor", Param: "cursor", Path: "next"}, nil)
	_, res := collect(t, `{"next":null,"items":[]}`, rc)
	if res.Next != "" || res.Records != 0 {
		t.Fatalf("res=%+v", res)
	}
	_, res = collect(t, `{"items":[],"next":"p2"}`, rc)
	if res.Next != "p2" {
		t.Fatalf("res=%+v", res)
	}
}

func TestJSONTopLevelArray(t *testing.T) {
	rc := jsonRecipe(t, ".", recipe.Pagination{}, nil)
	recs, _ := collect(t, `[{"o":"A","d":"B","t":"20DRY","p":1,"c":"GBP"}]`, rc)
	if len(recs) != 1 || recs[0].OriginRaw != "A" {
		t.Fatal(recs)
	}
}

func TestJSONExplode(t *testing.T) {
	rc := jsonRecipe(t, "lanes", recipe.Pagination{}, &recipe.Explode{Path: "containers"})
	rc.Columns.ContainerType, rc.Columns.Price = "", ""
	body := `{"lanes":[{"o":"A","d":"B","c":"INR","v":{"from":"f","to":"t"},"containers":{"40HC":3,"20DRY":1,"40DRY":2}}]}`
	recs, res := collect(t, body, rc)
	if res.Records != 3 {
		t.Fatalf("res=%+v", res)
	}
	got := []string{recs[0].ContainerTypeRaw, recs[1].ContainerTypeRaw, recs[2].ContainerTypeRaw}
	if strings.Join(got, ",") != "20DRY,40DRY,40HC" {
		t.Fatalf("explode order must be sorted for determinism, got %v", got)
	}
	if *recs[2].Price != 3 || recs[2].Currency != "INR" {
		t.Fatalf("rec=%+v", recs[2])
	}
}

func TestJSONMalformedIsError(t *testing.T) {
	rc := jsonRecipe(t, "items", recipe.Pagination{}, nil)
	_, err := Parse(context.Background(), strings.NewReader(`{"items":[{"o":1`), Options{Recipe: rc}, func(*record.RawRecord) error { return nil })
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestContextCancellationStopsParsing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	body := "Origin,Destination,Equipment,Rate,Currency,BAF,ValidFrom,ValidTo\n" + strings.Repeat("A,B,20GP,1,EUR,1,x,y\n", 1000)
	n := 0
	_, err := Parse(ctx, strings.NewReader(body), Options{Recipe: csvRecipe(t)}, func(*record.RawRecord) error {
		n++
		if n == 10 {
			cancel()
		}
		return nil
	})
	if err == nil || n > 11 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

// BenchmarkParseCSV is the pprof target (D-20). It parses the committed
// fathom fixture (20k rows) when present, else a generated equivalent.
func BenchmarkParseCSV(b *testing.B) {
	body := fathomCSV(b)
	rc := recipe.Recipe{SourceID: "fathom", Carrier: "FATHOM", URL: "http://x", Format: "csv",
		Columns: recipe.Columns{Origin: "origin", Destination: "destination", ContainerType: "type", Price: "price",
			Currency: "currency", Unit: "per_container", ValidFrom: "valid_from", ValidTo: "valid_to"},
		Hints: recipe.Hints{DecimalStyle: "us"}}
	if err := rc.Validate(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Parse(context.Background(), bytes.NewReader(body), Options{Recipe: rc}, func(*record.RawRecord) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func fathomCSV(b *testing.B) []byte {
	if p := filepath.Join("..", "..", "spark", "tests", "fixtures", "fathom_bulk.csv"); fileExists(p) {
		data, err := os.ReadFile(p)
		if err == nil {
			return data
		}
	}
	var buf bytes.Buffer
	buf.WriteString("origin,destination,type,price,currency,valid_from,valid_to\n")
	for i := 0; i < 20000; i++ {
		buf.WriteString("INMAA,NLRTM,40HC,2132.75,USD,2026-07-01,2026-12-31\n")
	}
	return buf.Bytes()
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }
