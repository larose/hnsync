# hnsync

`hnsync` is a Go application that syncs Hacker News items to a local SQLite database. It monitors new items in near real-time and periodically updates existing ones, providing a simple way to work with Hacker News data locally.

## Prerequisites

- Go: Version 1.22 or later
- SQLite: Version 3

## Usage

```bash
# Run with default settings
go run github.com/larose/hnsync@latest

# Customize settings
go run github.com/larose/hnsync@latest -workers 100 -db custom.db

# Enable profiling for performance monitoring
go run github.com/larose/hnsync@latest -profile
```

## Flags

- `-workers` (default: 200): Number of concurrent worker goroutines for syncing.
- `-db` (default: hn.db): Filename for the SQLite database.
- `-profile`: Enables a pprof server on :6060 for performance analysis.

## Schema

The synced data is stored in a table named `hn_items`, which contains the following key columns:

- `id`: Unique Hacker News item identifier (INTEGER).
- `data`: Raw JSON data from the Hacker News API (TEXT).
- `_last_synced_at`: Timestamp of the last successful sync (TEXT). Useful for tracking changes.

Additional internal columns prefixed with `_` are used for syncing operations and are not intended for direct use.

### Example Queries

Get one item:

```sql
sqlite> SELECT id, data FROM hn_items LIMIT 1;
id  data
--  ------------------------------------------------------------
1   {"by":"pg","descendants":15,"id":1,"kids":[15,234509,487171,
    82729],"score":57,"time":1160418111,"title":"Y Combinator","
    type":"story","url":"http://ycombinator.com"}
```

Retrieve items updated in the last hour:

```sql
sqlite> SELECT id, data FROM hn_items WHERE _last_synced_at > datetime('now', '-1 hour');
...
```

### Note on Indexing

By default, there are no indexes on the `_last_synced_at` column. For better query performance, especially when filtering by this column, consider adding an index:

```sql
CREATE INDEX hn_items_last_synced_at ON hn_items (_last_synced_at);
```

## License

This project is licensed under the GNU General Public License v3.0. Refer to the
`LICENSE` file for more details.

Copyright (C) 2024 Mathieu Larose
