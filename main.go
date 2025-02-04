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

	globalContext, cancelGlobalContext := context.WithCancel(context.Background())

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT)

	go func() {
		<-sigint
		fmt.Println("Shutting down")
		cancelGlobalContext()
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

	var producerGroup sync.WaitGroup
	refreshQueue := make(chan SyncItem, 100)

	maxItemId, err := getMaxItemID(db)
	if err != nil {
		log.Fatalf("Error getting max item ID: %v", err)
	}

	producerGroup.Add(1)
	go discoverer(db, &producerGroup, globalContext, httpClient, maxItemId)
	producerGroup.Add(1)
	go refresher(refreshQueue, db, &producerGroup, globalContext)

	var refreshProcessingCount atomic.Uint64

	progressContext, cancelProgressContext := context.WithCancel(context.Background())
	var progressGroup sync.WaitGroup
	go showProgress(&refreshProcessingCount, &progressGroup, progressContext, refreshQueue)

	var consumerGroup sync.WaitGroup

	for i := 0; i < config.NumWorkers; i++ {
		consumerGroup.Add(1)
		go syncer(refreshQueue, db, &consumerGroup, &refreshProcessingCount, httpClient)
	}

	log.Println("Processing")

	producerGroup.Wait()

	close(refreshQueue)

	consumerGroup.Wait()

	log.Println("Consumers finished")

	cancelProgressContext()

	progressGroup.Wait()
}
