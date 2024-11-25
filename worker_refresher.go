package main

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"
)

type SyncStatus int

const (
	Work SyncStatus = iota
	Sleep
)

func runEnqueueExistingItemsBatch(refreshQueue chan<- SyncItem, db *sql.DB) SyncStatus {
	itemIDs, err := getNextItemsToEnqueue(db)
	if err != nil {
		log.Printf("Failed to get next items to sync: %v", err)
		return Sleep
	}

	statement, err := createStatementToMakeItemNotVisible(db)
	if err != nil {
		log.Printf("Failed to create statement to hide items: %v", err)
		return Sleep
	}
	defer statement.Close()

	nextAvailableTime := time.Now().UTC().Add(5 * time.Minute)

	for _, itemId := range itemIDs {
		refreshQueue <- SyncItem{ID: itemId}

		_, err = statement.Exec(nextAvailableTime, itemId)

		if err != nil {
			log.Printf("Failed set item not visible: %v", err)
			continue
		}
	}

	if len(itemIDs) == 0 {
		return Sleep
	}

	return Work
}

func refresher(refreshQueue chan<- SyncItem, db *sql.DB, wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()

	for {
		status := runEnqueueExistingItemsBatch(refreshQueue, db)

		if status == Sleep {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
