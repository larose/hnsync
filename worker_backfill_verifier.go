package main

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"
)

func backillVerifier(
	newQueue chan<- SyncItem,
	waitGroup *sync.WaitGroup,
	ctx context.Context,
	db *sql.DB,
	upperBoundItemId uint64) {
	defer waitGroup.Done()

	// Set up ticker for periodic logging
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	done := make(chan struct{})
	defer close(done)

	log.Printf("Backfill - Verifying up to id %d", upperBoundItemId)

	var prevId uint64
	start := time.Now()
	batchSize := 1_000

	go func() {
		for {
			select {
			case <-ticker.C:
				percentCompleted := float64(prevId) / float64(upperBoundItemId) * 100
				log.Printf("Backfill - Current id: %d (%.0f%%)", prevId, percentCompleted)
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rows, err := selectItemsStartingAtId(db, prevId, upperBoundItemId, batchSize)
		if err != nil {
			log.Fatal("Backfill: query error:", err)
		}

		hasRows := false
		for rows.Next() {
			hasRows = true
			var currentId uint64
			if err := rows.Scan(&currentId); err != nil {
				log.Fatal("Backfill - scan error:", err)
			}

			// Send all gaps between prevId and currentId
			for id := prevId + 1; id < currentId; id++ {
				select {
				case newQueue <- SyncItem{ID: id}:
				case <-ctx.Done():
					rows.Close()
					return
				}
			}

			prevId = currentId
		}
		rows.Close()

		if err = rows.Err(); err != nil {
			log.Fatal("Backfill - iteration error:", err)
		}

		if !hasRows {
			break
		}
	}

	duration := time.Since(start)
	log.Printf("Backfill - Completed in %v seconds. Processed up to id: %d", duration.Round(time.Second), upperBoundItemId)
}
