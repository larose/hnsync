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

func runEnqueueExistingItemsBatch(refreshQueue chan<- SyncItem, hideItemStatement *sql.Stmt, getNextItemsToEnqueueStatement *sql.Stmt) SyncStatus {

	rows, err := getNextItemsToEnqueueStatement.Query()
	if err != nil {
		log.Printf("Failed to get next items to sync: %v", err)
		return Sleep
	}

	defer rows.Close()

	var itemIDs []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			log.Printf("Failed to scan item id: %v", err)
			continue
		}
		itemIDs = append(itemIDs, id)
	}

	nextAvailableTime := time.Now().UTC().Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	for _, itemId := range itemIDs {
		refreshQueue <- SyncItem{ID: itemId}

		_, err = hideItemStatement.Exec(nextAvailableTime, itemId)
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
	defer log.Println("Refresher finished")

	idleStartTime := time.Now()

	hideItemStatement, err := createHideItemStatement(db)
	if err != nil {
		log.Fatalf("Failed to create statement to hide items: %v", err)
	}
	defer hideItemStatement.Close()

	getNextItemsToEnqueueStatement, err := createGetNextItemsToEnqueueStatement(db)
	if err != nil {
		log.Fatalf("Failed to create statement to get next items to enqueue: %v", err)
	}
	defer getNextItemsToEnqueueStatement.Close()

	for {
		status := runEnqueueExistingItemsBatch(refreshQueue, hideItemStatement, getNextItemsToEnqueueStatement)

		if status == Work {
			idleStartTime = time.Now()
		} else if status == Sleep {
			if time.Since(idleStartTime) >= 10*time.Second {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
