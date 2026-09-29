/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_poolbackoff_test.go — PoolBackoff under concurrent use and markOfferingsUnavailable.

import (
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// The failure handler (poller goroutine) marks while GetInstanceTypes (scheduler goroutines)
// reads; the map must survive that under -race and every Mark must be observable by Until until
// it is cleared.
func TestPoolBackoffConcurrentMarkUntilClear(t *testing.T) {
	b := NewPoolBackoff(time.Hour)
	pools := []string{"pool1", "pool2", "pool3"}
	const workers, iterations = 8, 200

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				pool := pools[(w+i)%len(pools)]
				switch i % 3 {
				case 0:
					if until := b.Mark(pool); until.IsZero() {
						t.Errorf("Mark(%s) returned the zero time on an enabled backoff", pool)
					}
				case 1:
					b.Until(pool)
				default:
					b.Clear(pool)
				}
			}
		}(w)
	}
	wg.Wait()

	// Deterministic tail: a mark after the storm is held, a clear drops it.
	for _, pool := range pools {
		b.Mark(pool)
		if _, held := b.Until(pool); !held {
			t.Errorf("%s not held after Mark", pool)
		}
		b.Clear(pool)
		if _, held := b.Until(pool); held {
			t.Errorf("%s still held after Clear", pool)
		}
	}
	if n := len(b.until); n != 0 {
		t.Errorf("%d stale entries after clearing every pool", n)
	}
}

func TestMarkOfferingsUnavailable(t *testing.T) {
	multi := &cloudprovider.InstanceType{
		Name: "multi",
		Offerings: cloudprovider.Offerings{
			{Requirements: scheduling.NewRequirements(), Price: 1, Available: true},
			{Requirements: scheduling.NewRequirements(), Price: 2, Available: true},
		},
	}
	none := &cloudprovider.InstanceType{Name: "none"} // nil Offerings
	empty := &cloudprovider.InstanceType{Name: "empty", Offerings: cloudprovider.Offerings{}}

	its := []*cloudprovider.InstanceType{multi, none, empty}
	markOfferingsUnavailable(its) // must not panic on nil/empty offerings

	for _, o := range multi.Offerings {
		if o.Available {
			t.Errorf("offering price=%v still available", o.Price)
		}
	}
	if len(multi.Offerings.Available()) != 0 {
		t.Errorf("Available() = %d offerings, want 0", len(multi.Offerings.Available()))
	}
	if len(multi.Offerings) != 2 {
		t.Errorf("offerings were removed rather than flagged: %d left", len(multi.Offerings))
	}
	markOfferingsUnavailable(nil) // empty list is a no-op
}
