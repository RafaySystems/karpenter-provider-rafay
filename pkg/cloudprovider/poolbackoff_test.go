/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

import (
	"testing"
	"time"
)

func TestIsPoolAtMaxDetail(t *testing.T) {
	cases := map[string]bool{
		`batch add: pool at maximum: pool "pool1" has 3 of max 4 nodes; 2 requested, 1 refused`: true,
		`batch add: pool "pool1" is at its maximum: 3 + 2 would exceed max 4`:                   true,
		`Pool At Maximum`: true,
		"batch add: ApplyCluster: still conflicting after 3 attempts":             false,
		"batch expired at broker":                                                 false,
		`batch add: pool "pool1" is at its minimum: 1 - 1 would drop below min 1`: false,
		"": false,
	}
	for detail, want := range cases {
		if got := IsPoolAtMaxDetail(detail); got != want {
			t.Errorf("IsPoolAtMaxDetail(%q) = %v, want %v", detail, got, want)
		}
	}
}

func TestPoolBackoffMarkUntilExpiry(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	b := NewPoolBackoff(5 * time.Minute)
	b.now = func() time.Time { return now }

	if _, held := b.Until("pool1"); held {
		t.Fatal("unmarked pool must not be held")
	}
	if until := b.Mark("pool1"); !until.Equal(now.Add(5 * time.Minute)) {
		t.Fatalf("Mark returned %v, want now+5m", until)
	}
	if until, held := b.Until("pool1"); !held || !until.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("Until = %v/%v, want held until now+5m", until, held)
	}
	if _, held := b.Until("pool2"); held {
		t.Fatal("hold must be per pool")
	}

	// A second refusal extends the hold from its own time.
	now = now.Add(3 * time.Minute)
	b.Mark("pool1")
	now = now.Add(4 * time.Minute) // 7m after the first mark, 4m after the second
	if _, held := b.Until("pool1"); !held {
		t.Fatal("re-marked pool must still be held")
	}

	now = now.Add(2 * time.Minute) // past the second mark
	if _, held := b.Until("pool1"); held {
		t.Fatal("hold must expire on its own")
	}
	if len(b.until) != 0 {
		t.Errorf("expired entry not forgotten: %v", b.until)
	}
}

func TestPoolBackoffClear(t *testing.T) {
	b := NewPoolBackoff(time.Hour)
	b.Mark("pool1")
	b.Clear("pool1")
	if _, held := b.Until("pool1"); held {
		t.Fatal("Clear must drop the hold")
	}
}

func TestPoolBackoffDisabledAndNil(t *testing.T) {
	disabled := NewPoolBackoff(0)
	if until := disabled.Mark("pool1"); !until.IsZero() {
		t.Fatalf("disabled backoff must not record marks, got %v", until)
	}
	if _, held := disabled.Until("pool1"); held {
		t.Fatal("disabled backoff must never hold")
	}
	if b := (*PoolBackoff)(nil); b.Mark("pool1") != (time.Time{}) || b.Cooldown() != 0 {
		t.Fatal("nil backoff must be inert")
	}
	if _, held := (*PoolBackoff)(nil).Until("pool1"); held {
		t.Fatal("nil backoff must never hold")
	}
	(*PoolBackoff)(nil).Clear("pool1") // must not panic
	if until := NewPoolBackoff(time.Minute).Mark(""); !until.IsZero() {
		t.Fatal("an empty pool name must not be recorded")
	}
}
