package record

import (
	"testing"
	"time"
)

func sample() RawRecord {
	return RawRecord{
		SchemaVersion: SchemaVersion, RunID: "r1", SourceID: "meridian", Carrier: "MERIDIAN",
		FetchedAt: time.Now(), Page: 1, RowIndex: 3,
		OriginRaw: "INMAA", DestinationRaw: "NLRTM", ContainerTypeRaw: "40HC",
		PriceRaw: "2,310", Currency: "USD", Unit: "per_container",
		ValidFromRaw: "2026-07-01", ValidToRaw: "2026-12-31",
		Surcharges: []Surcharge{{Name: "BAF", AmountRaw: "120"}},
	}
}

func TestHashIgnoresRunMetadata(t *testing.T) {
	a := sample()
	b := sample()
	b.RunID = "other"
	b.FetchedAt = b.FetchedAt.Add(time.Hour)
	b.Page = 9
	b.RowIndex = 0
	if a.ComputeHash() != b.ComputeHash() {
		t.Fatal("hash must not depend on run_id, fetched_at, page or row_index")
	}
}

func TestHashChangesWithContent(t *testing.T) {
	a := sample()
	b := sample()
	b.PriceRaw = "2,311"
	if a.ComputeHash() == b.ComputeHash() {
		t.Fatal("different price must hash differently")
	}
	c := sample()
	c.Surcharges[0].AmountRaw = "121"
	if a.ComputeHash() == c.ComputeHash() {
		t.Fatal("different surcharge must hash differently")
	}
}

func TestHashTrimsWhitespace(t *testing.T) {
	a := sample()
	b := sample()
	b.OriginRaw = "  INMAA "
	if a.ComputeHash() != b.ComputeHash() {
		t.Fatal("surrounding whitespace must not change the hash")
	}
}

func TestManifestStatus(t *testing.T) {
	m := Manifest{Totals: Totals{SourcesOK: 8}}
	if m.Status() != "ok" {
		t.Fatal(m.Status())
	}
	m.Totals.SourcesFailed = 1
	if m.Status() != "partial" {
		t.Fatal(m.Status())
	}
}
