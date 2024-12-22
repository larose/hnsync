package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
)

func discoverer(
	db *sql.DB,
	waitGroup *sync.WaitGroup,
	ctx context.Context,
	client *http.Client,
	prevMaxItemId uint64) {
	waitGroup.Add(1)
	defer waitGroup.Done()
	defer log.Println("Discoverer finished")

	var maxItemId = prevMaxItemId

	log.Printf("Starting at id %d", prevMaxItemId+1)

	insertStatement, err := createInsertNewItemStatement(db)
	if err != nil {
		log.Fatalf("Failed to create insert statement: %v", err)
	}
	defer insertStatement.Close()

	if err := fetchMaxItem(&maxItemId, client); err != nil {
		log.Printf("Error fetching max item: %v\n", err)
	}

	log.Printf("Max item id: %d", maxItemId)

	for i := prevMaxItemId + 1; i <= maxItemId; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := insertStatement.Exec(i)
			if err != nil {
				log.Fatalf("Failed to insert item %d: %v", i, err)
			}
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
