package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
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

func computeNextSyncDuration(now, itemTime time.Time) time.Duration {
	itemAgeHours := now.Sub(itemTime).Hours()

	switch {
	case itemAgeHours < 1:
		return 20 * time.Minute
	case itemAgeHours < 3:
		return 1 * time.Hour
	case itemAgeHours < 24:
		return 3 * time.Hour
	case itemAgeHours < 7*24:
		return 24 * time.Hour
	default:
		days := time.Duration(90*24) * time.Hour              // 90 days
		jitter := time.Duration(rand.Intn(14*24)) * time.Hour // Between 0 and 14 days
		return days + jitter
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

func syncItem(id uint64, db *sql.DB, processingCount *atomic.Uint64, client *http.Client) error {
	processingCount.Add(1)

	needSync, err := itemNeedSync(db, id)
	if err != nil {
		return err
	}

	if !needSync {
		return nil
	}

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
	nextSyncDuration := computeNextSyncDuration(time.Now().UTC(), itemTime)

	return upsertItem(db, id, string(data), nextSyncDuration)
}

func syncer(newQueue <-chan SyncItem, refreshQueue <-chan SyncItem, db *sql.DB, wg *sync.WaitGroup, newProcessingCount *atomic.Uint64, refreshProcessingCount *atomic.Uint64, client *http.Client) {
	defer wg.Done()

	for newQueue != nil || refreshQueue != nil {
		select {
		case task, ok := <-newQueue:
			if !ok {
				newQueue = nil
			} else if err := syncItem(task.ID, db, newProcessingCount, client); err != nil {
				log.Printf("Failed to sync item %d: %v", task.ID, err)
			}
		default:
			select {
			case task, ok := <-refreshQueue:
				if !ok {
					refreshQueue = nil
				} else if err := syncItem(task.ID, db, refreshProcessingCount, client); err != nil {
					log.Printf("Failed to sync item %d: %v", task.ID, err)
				}
			default:
				time.Sleep(10 * time.Second)
			}
		}
	}
}
