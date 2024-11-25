package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

func discoverer(
	newQueue chan<- SyncItem,
	waitGroup *sync.WaitGroup,
	ctx context.Context,
	client *http.Client,
	prevMaxItem uint64) {
	defer waitGroup.Done()

	var maxItem = prevMaxItem

	log.Printf("Starting at id %d", prevMaxItem+1)

	for {
		if err := fetchMaxItem(&maxItem, client); err != nil {
			log.Printf("Error fetching max item: %v\n", err)
		}

		log.Printf("Max item id: %d", maxItem)

		for i := prevMaxItem + 1; i <= maxItem; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				newQueue <- SyncItem{ID: i}
			}
		}

		prevMaxItem = maxItem

		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			continue
		}
	}
}

func fetchMaxItem(maxItem *uint64, client *http.Client) error {
	resp, err := client.Get("https://hacker-news.firebaseio.com/v0/maxitem.json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(body, maxItem); err != nil {
		return err
	}

	return nil
}
