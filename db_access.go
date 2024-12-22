package main

import (
	"database/sql"
	_ "embed"
)

//go:embed schema.sql
var createTablesQuery string

func createInsertNewItemStatement(db *sql.DB) (*sql.Stmt, error) {
	insertQuery := `
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
            'null',
            CURRENT_TIMESTAMP,
            0,
            0,
            0
        );
    `
	return db.Prepare(insertQuery)
}

func createHideItemStatement(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`UPDATE hn_items SET _visible_at = ? WHERE id = ?`)
}

func createTables(db *sql.DB) error {
	_, err := db.Exec(createTablesQuery)
	return err
}

func createUpdateItemStatement(db *sql.DB) (*sql.Stmt, error) {
	upsertQuery := `
        UPDATE
			hn_items
		SET
			data = ?,
			_last_synced_at = CURRENT_TIMESTAMP,
			_next_sync_at = ?,
			_visible_at = 0
		WHERE
			id = ?;
    `
	return db.Prepare(upsertQuery)
}

func getMaxItemID(db *sql.DB) (uint64, error) {
	var maxItemInDb uint64
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM hn_items").Scan(&maxItemInDb)
	return maxItemInDb, err
}

func createGetNextItemsToEnqueueStatement(db *sql.DB) (*sql.Stmt, error) {
	selectNextItemsQuery := `
        SELECT
            id
        FROM
            hn_items
        WHERE
			_next_sync_at <= datetime('now') AND
			_visible_at <= datetime('now')
        LIMIT 100
    `

	return db.Prepare(selectNextItemsQuery)
}
