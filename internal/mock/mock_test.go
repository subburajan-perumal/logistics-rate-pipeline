package mock

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func fetchAll(t *testing.T, srv *Server) map[string][]byte {
	t.Helper()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	paths := []string{"/meridian/rates?page=1", "/meridian/rates?page=4", "/halcyon/tariff.csv", "/aurora/v2/lanes",
		"/aurora/v2/lanes?cursor=p2", "/borealis/rates.json", "/corvus/export.csv", "/delphine/rates?page=1",
		"/eventide/rates", "/fathom/bulk.csv.gz"}
	out := map[string][]byte{}
	for _, p := range paths {
		req, _ := http.NewRequest("GET", ts.URL+p, nil)
		req.Header.Set("X-Api-Key", "eventide-demo-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d %s", p, resp.StatusCode, b)
		}
		out[p] = b
	}
	return out
}

func TestDeterministicAcrossInstances(t *testing.T) {
	a := fetchAll(t, NewServer(Config{LatencyScale: 0}))
	b := fetchAll(t, NewServer(Config{LatencyScale: 0}))
	for p := range a {
		if !bytes.Equal(a[p], b[p]) {
			t.Fatalf("%s differs between instances", p)
		}
	}
	c := fetchAll(t, NewServer(Config{LatencyScale: 0, Seed: 1}))
	if bytes.Equal(a["/halcyon/tariff.csv"], c["/halcyon/tariff.csv"]) {
		t.Fatal("different seed must change the data")
	}
}

func TestEventideRequiresKey(t *testing.T) {
	ts := httptest.NewServer(NewServer(Config{LatencyScale: 0}).Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/eventide/rates")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal(resp.StatusCode)
	}
}

func TestDelphineFailureSchedule(t *testing.T) {
	ts := httptest.NewServer(NewServer(Config{LatencyScale: 0, Failures: true}).Handler())
	defer ts.Close()
	codes := []int{}
	for _, p := range []string{"?page=1", "?page=2", "?page=2", "?page=2", "?page=3"} { // n=1..5
		resp, _ := http.Get(ts.URL + "/delphine/rates" + p)
		codes = append(codes, resp.StatusCode)
		resp.Body.Close()
	}
	want := []int{200, 429, 500, 200, 200} // n%5==2 → 429, n%3==0 → 500
	for i := range want {
		if codes[i] != want[i] {
			t.Fatalf("codes=%v want %v", codes, want)
		}
	}
}

func TestExpectedTotals(t *testing.T) {
	if ExpectedTotal() != 20810 {
		t.Fatal(ExpectedTotal())
	}
	g := NewGen(DefaultSeed)
	if n := len(g.Lanes(spec("fathom"))); n != 500 {
		t.Fatal(n)
	}
	if len(Origins)*len(Destinations) != 500 {
		t.Fatalf("port table must give exactly 500 lanes, got %d", len(Origins)*len(Destinations))
	}
}
