//go:build linux

package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// peakRSSMB reads VmHWM (peak resident set) from /proc/self/status.
func peakRSSMB() float64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.ParseFloat(fields[1], 64)
				return kb / 1024
			}
		}
	}
	return 0
}
