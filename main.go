package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	DBFileName      string
	NumWorkers      int
	EnableProfiling bool
}

func parseFlags() Config {
	config := Config{}

	flag.StringVar(&config.DBFileName, "db", "hn.db", "SQLite database filename")
	flag.IntVar(&config.NumWorkers, "workers", 200, "Number of concurrent workers")
	flag.BoolVar(&config.EnableProfiling, "profile", false, "Enable pprof profiling server")

	flag.Parse()
	return config
}

func main() {
	config := parseFlags()

	if config.EnableProfiling {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	transport := &http.Transport{
		MaxIdleConns:        int(float64(config.NumWorkers) * 1.1), // Workers + 10%
		MaxIdleConnsPerHost: int(float64(config.NumWorkers) * 1.1),
		IdleConnTimeout:     30 * time.Second,
	}

	// Initialize the shared http.Client
	httpClient := &http.Client{
		Transport: transport,
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT)

	go func() {
		<-sigint
		fmt.Println("Shutting down")
		cancel()
	}()

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&_foreign_keys=true&_journal_mode=WAL&mode=rwc", config.DBFileName))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// https://github.com/mattn/go-sqlite3/issues/274
	db.SetMaxOpenConns(1)

	err = createTables(db)
	if err != nil {
		log.Fatalf("Error creating tables: %v", err)
	}

	var enqueueGroup sync.WaitGroup
	newQueue := make(chan SyncItem, 100)
	refreshQueue := make(chan SyncItem, 100)

	maxItemInDb, err := getMaxItemID(db)
	if err != nil {
		log.Fatalf("Error getting max item ID: %v", err)
	}

	startingId := max(0, maxItemInDb-min(maxItemInDb, 1_000))

	enqueueGroup.Add(1)
	go backillVerifier(newQueue, &enqueueGroup, ctx, db, startingId)

	enqueueGroup.Add(1)
	go discoverer(newQueue, &enqueueGroup, ctx, httpClient, startingId)

	enqueueGroup.Add(1)
	go refresher(refreshQueue, db, &enqueueGroup, ctx)

	var newProcessingCount atomic.Uint64
	var refreshProcessingCount atomic.Uint64

	enqueueGroup.Add(1)
	go showProgress(&newProcessingCount, &refreshProcessingCount, &enqueueGroup, ctx, newQueue, refreshQueue)

	var consumerGroup sync.WaitGroup

	for i := 0; i < config.NumWorkers; i++ {
		consumerGroup.Add(1)
		go syncer(newQueue, refreshQueue, db, &consumerGroup, &newProcessingCount, &refreshProcessingCount, httpClient)
	}

	log.Println("Processing")

	enqueueGroup.Wait()

	close(newQueue)
	close(refreshQueue)

	consumerGroup.Wait()
}
