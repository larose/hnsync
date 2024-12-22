package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	hnAPIURL        = "https://hacker-news.firebaseio.com/v0"
	itemURLTemplate = hnAPIURL + "/item/%d.json"
)

type hnItem struct {
	Time int64
}

type NextSync struct {
	Duration  time.Duration
	NeverSync bool
}

func computeNextSyncDuration(now, itemTime time.Time) NextSync {
	itemAgeHours := now.Sub(itemTime).Hours()

	switch {
	case itemAgeHours < 1:
		return NextSync{Duration: 20 * time.Minute}
	case itemAgeHours < 3:
		return NextSync{Duration: 1 * time.Hour}
	case itemAgeHours < 24:
		return NextSync{Duration: 3 * time.Hour}
	case itemAgeHours < 90*24:
		return NextSync{Duration: 24 * time.Hour}
	default:
		return NextSync{NeverSync: true}
	}
}

func downloadItem(id uint64, client *http.Client) ([]byte, error) {
	url := fmt.Sprintf(itemURLTemplate, id)
	resp, err := client.Get(url)
	if err != nil {
		return []byte{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return []byte{}, fmt.Errorf("failed to retrieve item: %s", resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}

	return bodyBytes, nil
}

func syncItem(id uint64, processingCount *atomic.Uint64, client *http.Client, updateItemStatement *sql.Stmt) error {
	processingCount.Add(1)

	data, err := downloadItem(id, client)
	if err != nil {
		return err
	}

	var item hnItem
	err = json.Unmarshal(data, &item)
	if err != nil {
		return err
	}

	itemTime := time.Unix(item.Time, 0).UTC()
	nextSync := computeNextSyncDuration(time.Now().UTC(), itemTime)

	var nextSyncTime string
	if nextSync.NeverSync {
		nextSyncTime = "3000-01-01 00:00:00"
	} else {
		nextSyncTime = time.Now().UTC().Add(nextSync.Duration).Format("2006-01-02 15:04:05")
	}

	_, err = updateItemStatement.Exec(string(data), nextSyncTime, id)

	return err
}

func syncer(refreshQueue <-chan SyncItem, db *sql.DB, wg *sync.WaitGroup, refreshProcessingCount *atomic.Uint64, client *http.Client) {
	defer wg.Done()

	updateItemStatement, err := createUpdateItemStatement(db)
	if err != nil {
		log.Fatalf("Failed to create statement to update items: %v", err)
	}

	for {
		select {
		case task, ok := <-refreshQueue:
			if !ok {
				return
			} else if err := syncItem(task.ID, refreshProcessingCount, client, updateItemStatement); err != nil {
				log.Printf("Failed to sync item %d: %v", task.ID, err)
			}
		default:
			time.Sleep(10 * time.Second)
		}
	}
}
