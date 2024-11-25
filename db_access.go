package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"time"
)

//go:embed schema.sql
var createTablesQuery string

func createStatementToMakeItemNotVisible(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`UPDATE hn_items SET _visible_at = ? WHERE id = ?`)
}

func createTables(db *sql.DB) error {
	_, err := db.Exec(createTablesQuery)
	return err
}

func getMaxItemID(db *sql.DB) (uint64, error) {
	var maxItemInDb uint64
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM hn_items").Scan(&maxItemInDb)
	return maxItemInDb, err
}

func getNextItemsToEnqueue(db *sql.DB) ([]uint64, error) {
	selectNextItemsQuery := `
        SELECT
            id
        FROM
            hn_items
        WHERE
			_next_sync_at <= datetime('now') AND
			_visible_at <= datetime('now')
        LIMIT 10
    `

	rows, err := db.Query(selectNextItemsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var itemIDs []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		itemIDs = append(itemIDs, id)
	}

	return itemIDs, nil
}

func itemNeedSync(db *sql.DB, id uint64) (bool, error) {
	var needSync bool

	err := db.QueryRow(`
        SELECT
            _next_sync_at <= datetime('now')
        FROM
            hn_items
        WHERE
            id = ?
        `, id).Scan(&needSync)

	if err != nil {
		if err != sql.ErrNoRows {
			return false, err
		}
		needSync = true
	}

	return needSync, nil
}

func selectItemsStartingAtId(db *sql.DB, start uint64, upperBoundItemId uint64, limit int) (*sql.Rows, error) {
	rows, err := db.Query(`
		SELECT
			id
		FROM
			hn_items
		WHERE
			id > ? AND
			id <= ?
		ORDER BY
			id
		LIMIT ?`, start, upperBoundItemId, limit)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func upsertItem(db *sql.DB, id uint64, data string, nextSyncDuration time.Duration) error {
	upsertQuery := `
        INSERT INTO
            hn_items (
                id,
                data,
                _created_at,
                _last_synced_at,
                _next_sync_at,
                _visible_at
            )
        VALUES (
            ?,
            ?,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            datetime(CURRENT_TIMESTAMP, ?),
            0
        )
        ON CONFLICT (id) DO UPDATE SET
            data = excluded.data,
            _last_synced_at = CURRENT_TIMESTAMP,
            _next_sync_at = datetime(CURRENT_TIMESTAMP, ?),
            _visible_at = excluded._visible_at;
    `
	statement, err := db.Prepare(upsertQuery)
	if err != nil {
		return err
	}
	defer statement.Close()

	interval := fmt.Sprintf("+%d seconds", int(nextSyncDuration.Seconds()))
	_, err = statement.Exec(id, data, interval, interval)
	return err
}
