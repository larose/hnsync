CREATE TABLE IF NOT EXISTS hn_items (
    id INTEGER NOT NULL PRIMARY KEY,
    data TEXT NOT NULL,
    _created_at TEXT NOT NULL,
    _last_synced_at TEXT NOT NULL,
    _next_sync_at TEXT NOT NULL,
    _visible_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS hn_items_next_sync ON hn_items (_next_sync_at);
