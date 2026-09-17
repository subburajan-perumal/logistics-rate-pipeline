# Latest results (regenerated view — the timestamped files are the data)

| metric | value | file |
|---|---|---|
| pool_speedup (median, 5 reps) | 1 worker 6.85 s → 8 workers 3.77 s = **1.82×** (D-40) | `20260917T070639Z-pool.md` |
| records_per_run | 20 810, 8/8 sources ok | fixture manifest |
| parse_bench (20k-row CSV) | 220 018 → 80 030 allocs/op (−64 %), 16.1 → 8.2 MB/op, ≈ −15 % ns/op | `20260917T071149Z-parse.md` |
| rows_out / dedup_removed / rejects | 2 294 / 18 450 / 66 (7 reasons) | `20260917T070639Z-normalize.json` |
| spark stage durations (local[2], 8 GB laptop) | read 4.7 s · transform 3.5 s · validate 1.1 s · dedup 0.8 s · write 2.8 s | same |
| race detector / goroutine leaks | clean / 0 (goleak) — windows/amd64, Go 1.27.0 | `go test -race ./...` |
| image_size_mb | not yet measured (no Docker on the build host); stripped windows binary is 21 MB | — |
| shutdown_records_lost | 0 in-process (`TestCancelMidRunWritesNoManifestAndNoParts`); in-cluster e2e pending kind | — |
| e2e_latency | pending Phases 7–9 | — |
