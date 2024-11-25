package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func showProgress(newProcessingCount *atomic.Uint64, refreshProcessingCount *atomic.Uint64, wg *sync.WaitGroup, ctx context.Context, newQueue <-chan SyncItem, refreshQueue <-chan SyncItem) {
	defer wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	previousNewProcessingCount := newProcessingCount.Load()
	previousRefreshProcessingCount := refreshProcessingCount.Load()
	lastShown := time.Now().UTC()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			currentNewProcessingCount := newProcessingCount.Load()
			currentRefreshProcessingCount := refreshProcessingCount.Load()

			deltaTime := now.Sub(lastShown)
			newDelta := currentNewProcessingCount - previousNewProcessingCount
			refreshDelta := currentRefreshProcessingCount - previousRefreshProcessingCount
			newProcessingRatePerSecond := fmt.Sprintf("%.2f", float64(newDelta)/deltaTime.Seconds())
			refreshProcessingRatePerSecond := fmt.Sprintf("%.2f", float64(refreshDelta)/deltaTime.Seconds())

			taskQueueLength := len(newQueue)
			refreshQueueLength := len(refreshQueue)

			log.Printf("Processing %s new items/second, %s refresh items/second, %d items in new queue, %d items in refresh queue\n", newProcessingRatePerSecond, refreshProcessingRatePerSecond, taskQueueLength, refreshQueueLength)

			previousNewProcessingCount = currentNewProcessingCount
			previousRefreshProcessingCount = currentRefreshProcessingCount
			lastShown = now
		}
	}
}
