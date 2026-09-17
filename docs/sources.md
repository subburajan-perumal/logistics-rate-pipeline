# The eight synthetic feeds

**Everything here is invented.** `mocksources` (`internal/mock`) generates
all eight carrier feeds from a seed (`20260917`) and a fixed port table;
the same request sequence returns the same bytes, the same latencies and
the same injected failures (D-07, D-09). Carrier names are fictional and
no real carrier's data, abbreviation or tariff structure is used.

Lanes are drawn from 10 Indian origin ports × 50 destinations = 500 pairs;
each carrier takes a seeded subset. Base prices are stable per (lane, type)
across carriers (so overlapping lanes look related) and each carrier
applies its own multiplier and currency.

| # | source_id | shape | rows | quirk it exists for | latency / failures |
|---|---|---|---|---|---|
| 1 | `meridian` | `GET /meridian/rates?page=N` — JSON `{items, next_page}`, 50/page | 180 (58 lanes × 3 + 6 superseded) | baseline paginated JSON; LOCODEs, USD, ISO dates | 150 ms/page |
| 2 | `halcyon` | `GET /halcyon/tariff.csv` — CSV | 120 | EUR, BAF surcharge column, EU decimals `1.198,56`, `dd/mm/yyyy`, equipment aliases `20GP/40GP/40HQ` | 300 ms |
| 3 | `aurora` | `GET /aurora/v2/lanes?cursor=…` — nested JSON `{lanes:[{…,containers:{20DRY:…}}], next_cursor}`, 25 lanes/page | 150 | nested object exploded into rows; INR; **slow** (1.5–2.5 s per page, fixed per page) | slow |
| 4 | `borealis` | `GET /borealis/rates.json` — top-level JSON array | 90 | **city names and aliases** (`JNPT`, `MADRAS`, `Abu Dhabi`) instead of LOCODEs; GBP | 200 ms |
| 5 | `corvus` | `GET /corvus/export.csv` — CSV | 90 (45 lanes × 2) | **per-TEU pricing** with sizes `20`/`40` | 200 ms |
| 6 | `delphine` | `GET /delphine/rates?page=N` — JSON, 3 pages | 105 | **flaky**: every 3rd request → 500, every 5th (mod 2) → 429 with `Retry-After: 2` | 100 ms + failures |
| 7 | `eventide` | `GET /eventide/rates` with `X-Api-Key` | 75 | **auth header from a Secret** (401 without); SGD/AED alternating by lane | 100 ms |
| 8 | `fathom` | `GET /fathom/bulk.csv.gz` — gzip CSV | 20 000 | streaming gzip parse; 1 500 distinct + 50 same-port + **18 450 duplicates** (exact, lower-cased or padded) shuffled in | 800 ms, ~3 MB gz |

## Trap rows (each is *inside* the counts above)

| source | rows | what | Spark rule that catches it |
|---|---|---|---|
| meridian | 6 | the first two lanes also appear with validity `2026-04-01 → 2026-06-30` (`MER-2026-Q2`) | `expired_as_of` |
| halcyon | 2 | first `20GP` row of lanes 0 and 1 has `ValidFrom`/`ValidTo` swapped | `dates_reversed` |
| borealis | 3 | lane 0's origin is `Springfield` | `unknown_port` |
| corvus | 2 | lane 0 size 20 → `-120`; lane 1 size 20 → blank | `bad_price` |
| delphine | 2 | lanes 0 and 1 quote `45HC` instead of `40HC` | `unknown_container` |
| eventide | 1 | lane 0 `20DRY` has currency `EURO` | `unknown_currency` |
| fathom | 50 | origin == destination | `same_port` |

Expected per run: **20 810** raw rows → **2 294** normalized (`MERIDIAN` 174,
`HALCYON` 118, `AURORA` 150, `BOREALIS` 87, `CORVUS` 88, `DELPHINE` 103,
`EVENTIDE` 74, `FATHOM` 1 500), **66** rejects across 7 reasons, **18 450**
duplicates removed. `spark/tests/fixtures/expected_counts.json` freezes these;
`make fixture` regenerates the raw fixture byte-for-byte.

## Why the speedup is what it is

The worker pool's wall time is bounded by the slowest source: `aurora`
needs two sequential cursor pages at ~1.9 s each (≈ 3.8 s), while the other
seven sum to ≈ 2.9 s. Sequential ≈ 6.7 s, parallel ≈ 3.8 s, so the ceiling
is ≈ 1.8× regardless of worker count — and the benchmark lands exactly
there (`bench/results/*-pool.md`). The plan's "≥ 4×" target assumed
per-source latencies that were never in the feed spec (D-40). The honest
next step would be page-level parallelism, which cursor pagination forbids
by design: the cursor for page N+1 is only known after page N is parsed.
