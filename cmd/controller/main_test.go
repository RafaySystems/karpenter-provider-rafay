/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package main

import (
	"testing"
	"time"
)

func TestMsEnvPortOrDefault(t *testing.T) {
	const name = "MS_TEST_PORT"
	for _, tc := range []struct {
		name  string
		value string
		set   bool
		want  int
	}{
		{"unset keeps default", "", false, 5448},
		{"empty keeps default", "", true, 5448},
		{"whitespace only keeps default", "   ", true, 5448},
		{"valid port", "5449", true, 5449},
		{"surrounding whitespace is trimmed", " 5449\n", true, 5449},
		{"non-numeric keeps default", "five", true, 5448},
		{"float keeps default", "5449.0", true, 5448},
		{"plus sign is accepted by Atoi", "+80", true, 80},
		// Pinned current domain: the helper does no range check. These only surface later as a
		// dial error; a range check would be a behaviour change.
		{"zero is accepted", "0", true, 0},
		{"negative is accepted", "-1", true, -1},
		{"above 65535 is accepted", "70000", true, 70000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(name, tc.value)
			}
			if got := envPortOrDefault(name, 5448); got != tc.want {
				t.Errorf("envPortOrDefault(%q=%q) = %d, want %d", name, tc.value, got, tc.want)
			}
		})
	}
}

func TestMsEnvDurationOrDefault(t *testing.T) {
	const name = "MS_TEST_DURATION"
	def := 5 * time.Minute
	for _, tc := range []struct {
		name  string
		value string
		set   bool
		want  time.Duration
	}{
		{"unset keeps default", "", false, def},
		{"empty keeps default", "", true, def},
		{"whitespace only keeps default", " \t", true, def},
		{"seconds", "90s", true, 90 * time.Second},
		{"compound", "1h30m", true, 90 * time.Minute},
		{"fractional", "1.5m", true, 90 * time.Second},
		{"surrounding whitespace is trimmed", "  2m ", true, 2 * time.Minute},
		{"bare number is not a duration", "300", true, def},
		{"typo keeps default", "5min", true, def},
		// Pinned current domain: negative and zero durations parse and are returned as-is;
		// the consumer (PoolBackoff) treats 0 as disabled.
		{"zero is accepted", "0s", true, 0},
		{"negative is accepted", "-5m", true, -5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(name, tc.value)
			}
			if got := envDurationOrDefault(name, def); got != tc.want {
				t.Errorf("envDurationOrDefault(%q=%q) = %s, want %s", name, tc.value, got, tc.want)
			}
		})
	}
}
