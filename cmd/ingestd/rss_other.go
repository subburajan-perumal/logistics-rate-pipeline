//go:build !linux

package main

import "runtime"

// peakRSSMB approximates peak memory with the Go runtime's Sys on platforms
// without /proc (the committed benchmarks come from Linux).
func peakRSSMB() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.Sys) / 1e6
}
