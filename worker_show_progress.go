package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func showProgress(refreshProcessingCount *atomic.Uint64, wg *sync.WaitGroup, ctx context.Context, refreshQueue <-chan SyncItem) {
	wg.Add(1)
	defer wg.Done()
	defer log.Println("Show progress finished")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	previousRefreshProcessingCount := refreshProcessingCount.Load()
	lastShown := time.Now().UTC()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()

			currentRefreshProcessingCount := refreshProcessingCount.Load()

			deltaTime := now.Sub(lastShown)

			refreshDelta := currentRefreshProcessingCount - previousRefreshProcessingCount
			refreshProcessingRatePerSecond := fmt.Sprintf("%.2f", float64(refreshDelta)/deltaTime.Seconds())

			refreshQueueLength := len(refreshQueue)

			log.Printf("Processing %s items/second, %d items in queue\n", refreshProcessingRatePerSecond, refreshQueueLength)

			previousRefreshProcessingCount = currentRefreshProcessingCount
			lastShown = now
		}
	}
}
